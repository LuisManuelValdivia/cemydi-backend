import {
  BadRequestException,
  Injectable,
  InternalServerErrorException,
  Logger,
  NotFoundException,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { spawn } from 'node:child_process';
import { Readable } from 'node:stream';
import { google } from 'googleapis';
import { PrismaService } from '../../prisma/prisma.service';

type BackupRecordSummary = {
  id: number;
  fileName: string;
  origin: BackupOrigin;
  sizeBytes: number;
  createdAt: string;
};

type BackupRecordWithLog = {
  backup: BackupRecordSummary;
  logText: string;
};

type BackupOrigin = 'MANUAL' | 'AUTOMATIC' | 'TABLE';

type BackupRecordRow = {
  id: number;
  fileName: string;
  origin: BackupOrigin;
  sizeBytes: number | bigint | string;
  createdAt: Date | string;
};

type BackupScheduleSummary = {
  enabled: boolean;
  everyDays: number;
  runAtTime: string;
  retentionDays: number;
  schemaName: string | null;
  lastRunAt: string | null;
  nextRunAt: string | null;
  createdAt: string;
  updatedAt: string;
};

type BackupScheduleRow = {
  id: number;
  isEnabled: boolean;
  intervalDays: number | bigint | string;
  runAtTime: string;
  retentionDays: number | bigint | string;
  schemaName: string | null;
  lastRunAt: Date | string | null;
  nextRunAt: Date | string | null;
  createdAt: Date | string;
  updatedAt: Date | string;
};

type ParsedDatabaseUrl = {
  host: string;
  port: string;
  username: string;
  password: string;
  database: string;
  sslMode?: string;
  channelBinding?: string;
};

type GoogleDriveProvider = 'service-account' | 'oauth2';

type GoogleDriveClientContext = {
  provider: GoogleDriveProvider;
  drive: ReturnType<typeof google.drive>;
};

type PgDumpResult = {
  content: Buffer;
  verboseLog: string;
};

@Injectable()
export class BackupsService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(BackupsService.name);
  private readonly managementSchema = 'management';
  private readonly backupSchemas = [
    'accounts',
    'catalog',
    'management',
  ] as const;
  private readonly scheduleDefaults = {
    enabled: false,
    everyDays: 1,
    runAtTime: '03:00',
    retentionDays: 7,
  } as const;
  private readonly scheduleRowId = 1;
  private readonly schedulerPollMs = 60_000;
  private readonly retentionPollMs = 60 * 60 * 1000;

  private schedulerTimer: NodeJS.Timeout | null = null;
  private schedulerInProgress = false;
  private lastRetentionSweepAt = 0;
  private backupScheduleSchemaColumnExists: boolean | null = null;

  constructor(private readonly prisma: PrismaService) {}

  onModuleInit() {
    this.schedulerTimer = setInterval(() => {
      void this.runAutomationTick();
    }, this.schedulerPollMs);

    void this.runAutomationTick();
  }

  onModuleDestroy() {
    if (this.schedulerTimer) {
      clearInterval(this.schedulerTimer);
      this.schedulerTimer = null;
    }
  }

  async createDatabaseBackupRecord() {
    return this.createAndStoreDatabaseBackupRecord('MANUAL');
  }

  async createSingleSchemaBackupRecord(schemaName: string) {
    return this.createAndStoreSingleSchemaBackupRecord(schemaName, 'TABLE');
  }

  async createSingleTableBackupRecord(tableName: string) {
    return this.createAndStoreSingleTableBackupRecord(tableName, 'TABLE');
  }

  async listDatabaseBackupRecords() {
    if (!(await this.tableExists('database_backups'))) {
      return [];
    }

    const rows = await this.prisma.$queryRaw<
      Array<{
        id: number;
        fileName: string;
        origin: BackupOrigin;
        sizeBytes: number | bigint | string;
        createdAt: Date | string;
      }>
    >`
      SELECT "id", "fileName", "origin", "sizeBytes", "createdAt"
      FROM "management"."database_backups"
      ORDER BY "createdAt" DESC, "id" DESC
    `;

    return rows.map((item) => this.mapBackupRecordSummary(item));
  }

  async getDatabaseBackupSchedule() {
    if (!(await this.tableExists('database_backup_schedule'))) {
      return this.mapBackupScheduleSummary(this.createDefaultScheduleRow());
    }

    const row = await this.getOrCreateBackupScheduleRow();
    return this.mapBackupScheduleSummary(row);
  }

  async updateDatabaseBackupSchedule(payload: Record<string, unknown>) {
    await this.ensureRequiredTable(
      'database_backup_schedule',
      'No se pudo guardar la programacion de respaldos porque la tabla database_backup_schedule no existe. Ejecuta las migraciones de Prisma.',
    );
    const existing = await this.getOrCreateBackupScheduleRow();
    const normalized = this.normalizeSchedulePayload(payload);
    const nextRunAt = normalized.enabled
      ? this.computeNextRunAt(
          new Date(),
          normalized.everyDays,
          normalized.runAtTime,
          this.parseOptionalDate(existing.lastRunAt),
        )
      : null;
    const now = new Date();

    const updatedRows = await this.upsertBackupScheduleRow(
      {
        id: this.scheduleRowId,
        isEnabled: normalized.enabled,
        intervalDays: normalized.everyDays,
        runAtTime: normalized.runAtTime,
        retentionDays: normalized.retentionDays,
        schemaName: normalized.schemaName,
        lastRunAt: this.parseOptionalDate(existing.lastRunAt),
        nextRunAt,
        createdAt: this.parseOptionalDate(existing.createdAt) ?? now,
        updatedAt: now,
      },
      true,
    );

    const updated = this.getBackupScheduleRow(this.toUnknownArray(updatedRows));
    if (!updated) {
      throw new InternalServerErrorException(
        'No se pudo guardar la programacion de respaldos',
      );
    }

    await this.applyRetentionPolicy(updated.retentionDays);
    void this.runAutomationTick();

    return this.mapBackupScheduleSummary(updated);
  }

  async deleteDatabaseBackupSchedule() {
    await this.ensureRequiredTable(
      'database_backup_schedule',
      'No se pudo eliminar la programacion de respaldos porque la tabla database_backup_schedule no existe. Ejecuta las migraciones de Prisma.',
    );
    const existing = await this.getOrCreateBackupScheduleRow();
    const now = new Date();

    const resetRows = await this.upsertBackupScheduleRow(
      {
        id: this.scheduleRowId,
        isEnabled: this.scheduleDefaults.enabled,
        intervalDays: this.scheduleDefaults.everyDays,
        runAtTime: this.scheduleDefaults.runAtTime,
        retentionDays: this.scheduleDefaults.retentionDays,
        schemaName: null,
        lastRunAt: null,
        nextRunAt: null,
        createdAt: this.parseOptionalDate(existing.createdAt) ?? now,
        updatedAt: now,
      },
      true,
    );

    const reset = this.getBackupScheduleRow(this.toUnknownArray(resetRows));
    if (!reset) {
      throw new InternalServerErrorException(
        'No se pudo eliminar la programacion automatica de respaldos',
      );
    }

    await this.applyRetentionPolicy(reset.retentionDays);
    void this.runAutomationTick();

    return this.mapBackupScheduleSummary(reset);
  }

  async getDatabaseBackupRecord(id: number) {
    await this.ensureRequiredTable(
      'database_backups',
      'No se pudo consultar el respaldo porque la tabla database_backups no existe. Ejecuta las migraciones de Prisma.',
    );

    const rows = await this.prisma.$queryRaw<BackupRecordRow[]>`
      SELECT "id", "fileName", "origin", "sizeBytes", "createdAt"
      FROM "management"."database_backups"
      WHERE "id" = ${id}
      LIMIT 1
    `;

    const record = rows[0];
    if (!record) {
      throw new NotFoundException('Respaldo no encontrado');
    }

    return {
      ...this.mapBackupRecordSummary(record),
      content: await this.downloadBackupFromDrive(record.fileName),
    };
  }

  async createDatabaseBackup() {
    const backup = await this.buildDirectDatabaseBackupPayload();

    return {
      fileName: backup.fileName,
      content: backup.content,
    };
  }

  async deleteDatabaseBackupRecord(id: number) {
    await this.ensureRequiredTable(
      'database_backups',
      'No se pudo eliminar el respaldo porque la tabla database_backups no existe. Ejecuta las migraciones de Prisma.',
    );

    const existingRows = await this.prisma.$queryRaw<BackupRecordRow[]>`
      SELECT "id", "fileName", "origin", "sizeBytes", "createdAt"
      FROM "management"."database_backups"
      WHERE "id" = ${id}
      LIMIT 1
    `;

    const existing = existingRows[0];
    if (!existing) {
      throw new NotFoundException('Respaldo no encontrado');
    }

    const deleted = await this.deleteBackupRecordAssets(existing);
    return this.mapBackupRecordSummary(deleted);
  }

  private async createAndStoreDatabaseBackupRecord(
    origin: BackupOrigin,
    options?: { applyRetention?: boolean },
  ): Promise<BackupRecordWithLog> {
    const backup = await this.buildDatabaseBackupPayload();
    let record: BackupRecordSummary;

    try {
      record = await this.insertBackupRecord({
        ...backup,
        origin,
      });
    } catch (error) {
      await this.tryDeleteUploadedBackupSilently(backup.fileName);
      throw error;
    }

    if (options?.applyRetention) {
      await this.applyRetentionPolicy();
    }

    return {
      backup: record,
      logText: this.composeBackupExecutionLog({
        backupType: 'database',
        fileName: backup.fileName,
        sizeBytes: backup.sizeBytes,
        requestedAt: backup.createdAt,
        backupCreatedAt: record.createdAt,
        provider: this.getGoogleDriveProvider(),
        verboseLog: backup.verboseLog,
        recordId: record.id,
      }),
    };
  }

  private async createAndStoreSingleSchemaBackupRecord(
    schemaName: string,
    origin: BackupOrigin,
  ): Promise<BackupRecordWithLog> {
    const backup = await this.buildSingleSchemaBackupPayload(schemaName);
    let record: BackupRecordSummary;

    try {
      record = await this.insertBackupRecord({
        ...backup,
        origin,
      });
    } catch (error) {
      await this.tryDeleteUploadedBackupSilently(backup.fileName);
      throw error;
    }

    return {
      backup: record,
      logText: this.composeBackupExecutionLog({
        backupType: 'schema',
        schemaName,
        fileName: backup.fileName,
        sizeBytes: backup.sizeBytes,
        requestedAt: backup.createdAt,
        backupCreatedAt: record.createdAt,
        provider: this.getGoogleDriveProvider(),
        verboseLog: backup.verboseLog,
        recordId: record.id,
      }),
    };
  }

  private async createAndStoreSingleTableBackupRecord(
    tableName: string,
    origin: BackupOrigin,
  ): Promise<BackupRecordWithLog> {
    const backup = await this.buildSingleTableBackupPayload(tableName);
    let record: BackupRecordSummary;

    try {
      record = await this.insertBackupRecord({
        ...backup,
        origin,
      });
    } catch (error) {
      await this.tryDeleteUploadedBackupSilently(backup.fileName);
      throw error;
    }

    return {
      backup: record,
      logText: this.composeBackupExecutionLog({
        backupType: 'table',
        tableName,
        fileName: backup.fileName,
        sizeBytes: backup.sizeBytes,
        requestedAt: backup.createdAt,
        backupCreatedAt: record.createdAt,
        provider: this.getGoogleDriveProvider(),
        verboseLog: backup.verboseLog,
        recordId: record.id,
      }),
    };
  }

  private async runAutomationTick() {
    if (this.schedulerInProgress) {
      return;
    }

    this.schedulerInProgress = true;

    try {
      const [hasScheduleTable, hasBackupsTable] = await Promise.all([
        this.tableExists('database_backup_schedule'),
        this.tableExists('database_backups'),
      ]);
      if (!hasScheduleTable || !hasBackupsTable) {
        return;
      }

      const schedule = await this.getOrCreateBackupScheduleRow();
      const now = new Date();

      await this.ensurePersistedNextRunAt(schedule, now);

      if (schedule.isEnabled) {
        const dueAt = this.parseOptionalDate(schedule.nextRunAt);
        if (dueAt && dueAt.getTime() <= now.getTime()) {
          this.logger.log(
            `Iniciando respaldo automatico programado para ${schedule.runAtTime} cada ${this.toSafeNumber(schedule.intervalDays)} dia(s)`,
          );

          const createdBackup = schedule.schemaName
            ? await this.createAndStoreSingleSchemaBackupRecord(
                schedule.schemaName,
                'AUTOMATIC',
              )
            : await this.createAndStoreDatabaseBackupRecord('AUTOMATIC', {
                applyRetention: true,
              });
          const executedAt = new Date(createdBackup.backup.createdAt);
          const refreshedSchedule = await this.getOrCreateBackupScheduleRow();
          const nextRunAt = this.computeNextRunAt(
            executedAt,
            this.toSafeNumber(refreshedSchedule.intervalDays),
            refreshedSchedule.runAtTime,
            executedAt,
          );

          await this.updateScheduleExecutionDates(
            refreshedSchedule.id,
            executedAt,
            nextRunAt,
          );

          this.logger.log(
            `Respaldo automatico completado: ${createdBackup.backup.fileName}`,
          );
          await this.applyRetentionPolicy();
        }
      }

      if (now.getTime() - this.lastRetentionSweepAt >= this.retentionPollMs) {
        await this.applyRetentionPolicy(
          this.toSafeNumber(schedule.retentionDays),
        );
        this.lastRetentionSweepAt = now.getTime();
      }
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : 'Error desconocido en el programador';
      this.logger.error(
        `No se pudo ejecutar el programador de respaldos: ${message}`,
      );
    } finally {
      this.schedulerInProgress = false;
    }
  }

  private async ensurePersistedNextRunAt(
    schedule: BackupScheduleRow,
    now: Date,
  ) {
    if (!schedule.isEnabled) {
      if (schedule.nextRunAt) {
        await this.updateScheduleExecutionDates(
          schedule.id,
          this.parseOptionalDate(schedule.lastRunAt),
          null,
        );
      }
      return;
    }

    const nextRunAt = this.parseOptionalDate(schedule.nextRunAt);
    if (nextRunAt) {
      return;
    }

    const computedNextRunAt = this.computeNextRunAt(
      now,
      this.toSafeNumber(schedule.intervalDays),
      schedule.runAtTime,
      this.parseOptionalDate(schedule.lastRunAt),
    );

    await this.updateScheduleExecutionDates(
      schedule.id,
      this.parseOptionalDate(schedule.lastRunAt),
      computedNextRunAt,
    );
  }

  private async updateScheduleExecutionDates(
    id: number,
    lastRunAt: Date | null,
    nextRunAt: Date | null,
  ) {
    await this.prisma.$queryRaw`
      UPDATE "management"."database_backup_schedule"
      SET
        "lastRunAt" = ${lastRunAt},
        "nextRunAt" = ${nextRunAt},
        "updatedAt" = ${new Date()}
      WHERE "id" = ${id}
    `;
  }

  private async getOrCreateBackupScheduleRow(): Promise<BackupScheduleRow> {
    if (!(await this.tableExists('database_backup_schedule'))) {
      return this.createDefaultScheduleRow();
    }

    const existingRows = await this.selectBackupScheduleRows();

    const existing = this.getBackupScheduleRow(
      this.toUnknownArray(existingRows),
    );
    if (existing) {
      return existing;
    }

    const now = new Date();
    const createdRows = await this.upsertBackupScheduleRow(
      {
        id: this.scheduleRowId,
        isEnabled: this.scheduleDefaults.enabled,
        intervalDays: this.scheduleDefaults.everyDays,
        runAtTime: this.scheduleDefaults.runAtTime,
        retentionDays: this.scheduleDefaults.retentionDays,
        schemaName: null,
        lastRunAt: null,
        nextRunAt: null,
        createdAt: now,
        updatedAt: now,
      },
      false,
    );

    return (
      this.getBackupScheduleRow(this.toUnknownArray(createdRows)) ??
      this.getOrCreateBackupScheduleRow()
    );
  }

  private mapBackupScheduleSummary(
    row: BackupScheduleRow,
  ): BackupScheduleSummary {
    return {
      enabled: row.isEnabled,
      everyDays: this.toSafeNumber(row.intervalDays),
      runAtTime: row.runAtTime,
      retentionDays: this.toSafeNumber(row.retentionDays),
      schemaName: row.schemaName,
      lastRunAt: this.toIsoString(row.lastRunAt),
      nextRunAt: this.toIsoString(row.nextRunAt),
      createdAt: this.toIsoString(row.createdAt) ?? new Date().toISOString(),
      updatedAt: this.toIsoString(row.updatedAt) ?? new Date().toISOString(),
    };
  }

  private normalizeSchedulePayload(payload: Record<string, unknown>) {
    const schemaName = this.normalizeOptionalSchemaName(payload.schemaName);

    return {
      enabled: this.normalizeBoolean(
        payload.enabled,
        'activar los respaldos automaticos',
      ),
      everyDays: this.normalizePositiveInteger(
        payload.everyDays,
        'la frecuencia en dias',
        365,
      ),
      runAtTime: this.normalizeTimeValue(payload.runAtTime),
      retentionDays: this.normalizePositiveInteger(
        payload.retentionDays,
        'la retencion de respaldos',
        3650,
      ),
      schemaName,
    };
  }

  private normalizeBoolean(value: unknown, fieldLabel: string) {
    if (typeof value === 'boolean') {
      return value;
    }

    if (typeof value === 'string') {
      const normalized = value.trim().toLowerCase();
      if (['true', '1', 'si', 'yes'].includes(normalized)) {
        return true;
      }

      if (['false', '0', 'no'].includes(normalized)) {
        return false;
      }
    }

    throw new BadRequestException(`Valor invalido para ${fieldLabel}`);
  }

  private normalizePositiveInteger(
    value: unknown,
    fieldLabel: string,
    maxValue: number,
  ) {
    const rawValue =
      typeof value === 'string'
        ? value.trim()
        : typeof value === 'number'
          ? value
          : NaN;
    const parsed = Number(rawValue);

    if (!Number.isInteger(parsed) || parsed < 1 || parsed > maxValue) {
      throw new BadRequestException(
        `Valor invalido para ${fieldLabel}. Debe ser un entero entre 1 y ${maxValue}`,
      );
    }

    return parsed;
  }

  private normalizeTimeValue(value: unknown) {
    if (typeof value !== 'string') {
      throw new BadRequestException(
        'La hora programada debe tener formato HH:mm en horario de 24 horas',
      );
    }

    const normalized = value.trim();

    if (!/^([01]\d|2[0-3]):[0-5]\d$/.test(normalized)) {
      throw new BadRequestException(
        'La hora programada debe tener formato HH:mm en horario de 24 horas',
      );
    }

    return normalized;
  }

  private computeNextRunAt(
    now: Date,
    everyDays: number,
    runAtTime: string,
    lastRunAt?: Date | null,
  ) {
    if (lastRunAt) {
      const nextRunAt = this.buildDateAtTime(lastRunAt, runAtTime);
      nextRunAt.setDate(nextRunAt.getDate() + everyDays);

      while (nextRunAt.getTime() <= now.getTime()) {
        nextRunAt.setDate(nextRunAt.getDate() + everyDays);
      }

      return nextRunAt;
    }

    const nextRunAt = this.buildDateAtTime(now, runAtTime);
    if (nextRunAt.getTime() <= now.getTime()) {
      nextRunAt.setDate(nextRunAt.getDate() + everyDays);
    }

    return nextRunAt;
  }

  private buildDateAtTime(baseDate: Date, runAtTime: string) {
    const [hours, minutes] = runAtTime.split(':').map((value) => Number(value));
    const scheduledDate = new Date(baseDate);
    scheduledDate.setHours(hours, minutes, 0, 0);
    return scheduledDate;
  }

  private async applyRetentionPolicy(
    retentionDaysOverride?: number | bigint | string,
  ) {
    if (!(await this.tableExists('database_backups'))) {
      return 0;
    }

    const retentionDays =
      retentionDaysOverride !== undefined
        ? this.toSafeNumber(retentionDaysOverride)
        : this.toSafeNumber(
            (await this.getOrCreateBackupScheduleRow()).retentionDays,
          );

    if (retentionDays < 1) {
      return 0;
    }

    const cutoffDate = new Date(
      Date.now() - retentionDays * 24 * 60 * 60 * 1000,
    );
    const expiredBackups = await this.prisma.$queryRaw<BackupRecordRow[]>`
      SELECT "id", "fileName", "origin", "sizeBytes", "createdAt"
      FROM "management"."database_backups"
      WHERE "createdAt" < ${cutoffDate}
        AND "origin"::text = ${'AUTOMATIC'}
      ORDER BY "createdAt" ASC, "id" ASC
    `;

    let deletedCount = 0;

    for (const backup of expiredBackups) {
      try {
        await this.deleteBackupRecordAssets(backup);
        deletedCount += 1;
      } catch (error) {
        const message =
          error instanceof Error
            ? error.message
            : 'Error desconocido al aplicar retencion';
        this.logger.warn(
          `No se pudo eliminar el respaldo vencido ${backup.fileName}: ${message}`,
        );
      }
    }

    if (deletedCount > 0) {
      this.logger.log(
        `Se eliminaron ${deletedCount} respaldo(s) vencidos por retencion automatica`,
      );
    }

    return deletedCount;
  }

  private async buildDatabaseBackupPayload() {
    const now = new Date();
    const fileName = this.createBackupFileName(now);
    const dump = await this.runPgDump();
    const content = dump.content;
    await this.uploadBackupToDrive(fileName, content);

    return {
      fileName,
      sizeBytes: content.length,
      createdAt: now,
      verboseLog: dump.verboseLog,
    };
  }

  private async buildSingleTableBackupPayload(rawTableName: string) {
    const tableName = this.normalizeTableName(rawTableName);
    const availableTables = await this.listBackupTableNames();

    if (!availableTables.includes(tableName)) {
      throw new NotFoundException('La tabla seleccionada no existe');
    }

    const now = new Date();
    const fileName = this.createTableBackupFileName(now, tableName);
    const dump = await this.runPgDump(tableName);
    const content = dump.content;
    await this.uploadBackupToDrive(fileName, content);

    return {
      fileName,
      sizeBytes: content.length,
      createdAt: now,
      verboseLog: dump.verboseLog,
    };
  }

  private async buildSingleSchemaBackupPayload(rawSchemaName: string) {
    const schemaName = this.normalizeSchemaName(rawSchemaName);
    const availableSchemas = await this.listBackupSchemaNames();

    if (!availableSchemas.includes(schemaName)) {
      throw new NotFoundException('El esquema seleccionado no existe');
    }

    const now = new Date();
    const fileName = this.createSchemaBackupFileName(now, schemaName);
    const dump = await this.runPgDump(undefined, schemaName);
    const content = dump.content;
    await this.uploadBackupToDrive(fileName, content);

    return {
      fileName,
      sizeBytes: content.length,
      createdAt: now,
      verboseLog: dump.verboseLog,
    };
  }

  private async buildDirectDatabaseBackupPayload() {
    const now = new Date();
    return {
      fileName: this.createBackupFileName(now),
      content: await this.runPgDump(),
    };
  }

  private mapBackupRecordSummary(item: {
    id: number;
    fileName: string;
    origin: BackupOrigin;
    sizeBytes: number | bigint | string;
    createdAt: Date | string;
  }): BackupRecordSummary {
    return {
      id: item.id,
      fileName: item.fileName,
      origin: item.origin,
      sizeBytes: this.toSafeNumber(item.sizeBytes),
      createdAt:
        item.createdAt instanceof Date
          ? item.createdAt.toISOString()
          : new Date(item.createdAt).toISOString(),
    };
  }

  private createBackupFileName(date: Date) {
    const timestamp = this.createBackupTimestamp(date);
    return `cemydi_backup_${timestamp.date}_${timestamp.time}.tar`;
  }

  private createTableBackupFileName(date: Date, tableName: string) {
    const timestamp = this.createBackupTimestamp(date);
    const safeTableName = tableName.replace(/\./g, '_');
    return `cemydi_${safeTableName}_backup_${timestamp.date}_${timestamp.time}.tar`;
  }

  private createSchemaBackupFileName(date: Date, schemaName: string) {
    const timestamp = this.createBackupTimestamp(date);
    return `cemydi_${schemaName}_schema_backup_${timestamp.date}_${timestamp.time}.tar`;
  }

  private createBackupTimestamp(date: Date) {
    const year = String(date.getFullYear());
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hour = String(date.getHours()).padStart(2, '0');
    const minute = String(date.getMinutes()).padStart(2, '0');
    const second = String(date.getSeconds()).padStart(2, '0');
    const millisecond = String(date.getMilliseconds()).padStart(3, '0');

    return {
      date: `${year}${month}${day}`,
      time: `${hour}${minute}${second}${millisecond}`,
    };
  }

  async getDatabaseStatus() {
    const hasPrismaMigrationsTable =
      await this.tableExists('_prisma_migrations');
    const [runtimeRows, tableRows, indexRows, connectionRows, initializedRows] =
      await Promise.all([
        this.prisma.$queryRaw<
          Array<{
            databaseName: string;
            version: string;
            sizeBytes: bigint | number | string;
            totalIndexes: number;
          }>
        >`
          SELECT
            current_database() AS "databaseName",
            version() AS "version",
            pg_database_size(current_database()) AS "sizeBytes",
            (
              SELECT COUNT(*)::int
              FROM pg_indexes
              WHERE schemaname IN ('accounts', 'catalog', 'management')
                AND NOT (
                  schemaname = 'management'
                  AND tablename = '_prisma_migrations'
                )
            ) AS "totalIndexes"
        `,
        this.prisma.$queryRaw<
          Array<{
            tableName: string;
            rowCount: bigint | number | string;
            sequentialScans: bigint | number | string;
            indexScans: bigint | number | string;
            totalQueries: bigint | number | string;
            totalSizeBytes: bigint | number | string;
            tableSizeBytes: bigint | number | string;
            indexSizeBytes: bigint | number | string;
            indexUsagePercent: number | string;
          }>
        >`
          SELECT
            (stats.schemaname || '.' || stats.relname) AS "tableName",
            COALESCE(stats.n_live_tup, 0)::bigint AS "rowCount",
            COALESCE(stats.seq_scan, 0)::bigint AS "sequentialScans",
            COALESCE(stats.idx_scan, 0)::bigint AS "indexScans",
            (COALESCE(stats.seq_scan, 0) + COALESCE(stats.idx_scan, 0))::bigint AS "totalQueries",
            pg_total_relation_size(stats.relid)::bigint AS "totalSizeBytes",
            pg_relation_size(stats.relid)::bigint AS "tableSizeBytes",
            pg_indexes_size(stats.relid)::bigint AS "indexSizeBytes",
            CASE
              WHEN (COALESCE(stats.seq_scan, 0) + COALESCE(stats.idx_scan, 0)) > 0 THEN
                ROUND(
                  (
                    COALESCE(stats.idx_scan, 0)::numeric /
                    (COALESCE(stats.seq_scan, 0) + COALESCE(stats.idx_scan, 0))::numeric
                  ) * 100,
                  2
                )
              ELSE 0::numeric
            END AS "indexUsagePercent"
          FROM pg_stat_user_tables AS stats
          WHERE stats.schemaname IN ('accounts', 'catalog', 'management')
            AND NOT (
              stats.schemaname = 'management'
              AND stats.relname = '_prisma_migrations'
            )
          ORDER BY
            pg_total_relation_size(stats.relid) DESC,
            stats.schemaname ASC,
            stats.relname ASC
        `,
        this.prisma.$queryRaw<
          Array<{
            indexName: string;
            tableName: string;
            scans: bigint | number | string;
            sizeBytes: bigint | number | string;
          }>
        >`
          SELECT
            (index_stats.schemaname || '.' || index_stats.indexrelname) AS "indexName",
            (index_stats.schemaname || '.' || index_stats.relname) AS "tableName",
            COALESCE(index_stats.idx_scan, 0)::bigint AS "scans",
            pg_relation_size(index_stats.indexrelid)::bigint AS "sizeBytes"
          FROM pg_stat_user_indexes AS index_stats
          WHERE index_stats.schemaname IN ('accounts', 'catalog', 'management')
            AND NOT (
              index_stats.schemaname = 'management'
              AND index_stats.relname = '_prisma_migrations'
            )
          ORDER BY
            COALESCE(index_stats.idx_scan, 0) DESC,
            pg_relation_size(index_stats.indexrelid) DESC,
            index_stats.indexrelname ASC
          LIMIT 10
        `,
        this.prisma.$queryRaw<
          Array<{
            pid: number;
            userName: string;
            state: string | null;
            clientAddress: string | null;
            applicationName: string | null;
            backendType: string | null;
          }>
        >`
          SELECT
            pid::int AS "pid",
            COALESCE(usename, 'desconocido') AS "userName",
            state AS "state",
            CASE
              WHEN client_addr IS NULL AND backend_type = 'client backend' THEN 'local'
              WHEN client_addr IS NULL THEN 'interna'
              ELSE client_addr::text
            END AS "clientAddress",
            NULLIF(application_name, '') AS "applicationName",
            backend_type AS "backendType"
          FROM pg_stat_activity
          WHERE datname = current_database()
          ORDER BY
            CASE
              WHEN state = 'active' THEN 0
              WHEN state = 'idle in transaction' THEN 1
              WHEN state = 'idle' THEN 2
              ELSE 3
            END,
            pid ASC
        `,
        hasPrismaMigrationsTable
          ? this.prisma.$queryRaw<
              Array<{
                initializedAt: Date | string | null;
              }>
            >`
              SELECT
                COALESCE(MIN("finished_at"), MIN("started_at")) AS "initializedAt"
              FROM "management"."_prisma_migrations"
            `
          : Promise.resolve([{ initializedAt: null }]),
      ]);

    const runtime = runtimeRows[0];
    const initializedAt = this.parseOptionalDate(
      initializedRows[0]?.initializedAt ?? null,
    );
    const sizeBytes = this.toSafeNumber(runtime?.sizeBytes);
    const tableStats = tableRows.map((item) => {
      const totalSizeBytes = this.toSafeNumber(item.totalSizeBytes);
      const tableSizeBytes = this.toSafeNumber(item.tableSizeBytes);
      const indexSizeBytes = this.toSafeNumber(item.indexSizeBytes);
      const sequentialScans = this.toSafeNumber(item.sequentialScans);
      const indexScans = this.toSafeNumber(item.indexScans);
      const totalQueries = this.toSafeNumber(item.totalQueries);
      const indexUsagePercent = Number(item.indexUsagePercent ?? 0);

      return {
        tableName: item.tableName,
        rowCount: this.toSafeNumber(item.rowCount),
        sequentialScans,
        indexScans,
        totalQueries,
        totalSizeBytes,
        totalSizePretty: this.formatBytes(totalSizeBytes),
        tableSizeBytes,
        tableSizePretty: this.formatBytes(tableSizeBytes),
        indexSizeBytes,
        indexSizePretty: this.formatBytes(indexSizeBytes),
        indexUsagePercent: Number.isFinite(indexUsagePercent)
          ? Math.max(0, Math.min(100, indexUsagePercent))
          : 0,
      };
    });
    const topQueriedTables = [...tableStats]
      .sort(
        (a, b) =>
          b.totalQueries - a.totalQueries ||
          b.totalSizeBytes - a.totalSizeBytes,
      )
      .slice(0, 8);
    const indexStats = indexRows.map((item) => {
      const scans = this.toSafeNumber(item.scans);
      const indexSizeBytes = this.toSafeNumber(item.sizeBytes);

      return {
        indexName: item.indexName,
        tableName: item.tableName,
        scans,
        sizeBytes: indexSizeBytes,
        sizePretty: this.formatBytes(indexSizeBytes),
      };
    });
    let activeConnections = 0;
    let idleConnections = 0;
    let idleInTransactionConnections = 0;
    let otherConnections = 0;
    let internalConnections = 0;

    const connectionItems = connectionRows.map((item) => {
      const backendType = item.backendType?.trim() || 'client backend';
      const rawState = item.state?.trim().toLowerCase() || 'unknown';
      const normalizedState =
        backendType !== 'client backend'
          ? 'internal'
          : rawState === 'active'
            ? 'active'
            : rawState === 'idle'
              ? 'idle'
              : rawState === 'idle in transaction'
                ? 'idle in transaction'
                : 'other';

      if (normalizedState === 'active') {
        activeConnections += 1;
      } else if (normalizedState === 'idle') {
        idleConnections += 1;
      } else if (normalizedState === 'idle in transaction') {
        idleInTransactionConnections += 1;
      } else if (normalizedState === 'internal') {
        internalConnections += 1;
      } else {
        otherConnections += 1;
      }

      return {
        pid: item.pid,
        userName: item.userName,
        state: normalizedState,
        clientAddress: item.clientAddress?.trim() || 'desconocida',
        applicationName: item.applicationName?.trim() || 'Sin etiqueta',
        backendType,
      };
    });
    const groupedUsers = new Map<
      string,
      {
        userName: string;
        totalConnections: number;
        activeConnections: number;
        internalConnections: number;
      }
    >();
    for (const connection of connectionItems) {
      const existing = groupedUsers.get(connection.userName) ?? {
        userName: connection.userName,
        totalConnections: 0,
        activeConnections: 0,
        internalConnections: 0,
      };
      existing.totalConnections += 1;
      if (connection.state === 'active') {
        existing.activeConnections += 1;
      }
      if (connection.state === 'internal') {
        existing.internalConnections += 1;
      }
      groupedUsers.set(connection.userName, existing);
    }
    const databaseUsers = [...groupedUsers.values()].sort(
      (a, b) =>
        b.totalConnections - a.totalConnections ||
        b.activeConnections - a.activeConnections ||
        a.userName.localeCompare(b.userName),
    );

    const totalRows = tableStats.reduce((acc, item) => acc + item.rowCount, 0);
    const totalTableBytes = tableStats.reduce(
      (acc, item) => acc + item.totalSizeBytes,
      0,
    );
    const databaseAgeSeconds = initializedAt
      ? Math.max(0, Math.floor((Date.now() - initializedAt.getTime()) / 1000))
      : 0;

    return {
      checkedAt: new Date().toISOString(),
      isOnline: true,
      databaseName: runtime?.databaseName ?? 'unknown',
      dbVersion: runtime?.version ?? 'unknown',
      initializedAt: initializedAt?.toISOString() ?? null,
      databaseAgeSeconds,
      sizeBytes,
      sizePretty: this.formatBytes(sizeBytes),
      overview: {
        totalTables: tableStats.length,
        totalIndexes: runtime?.totalIndexes ?? indexStats.length,
        totalRows,
        totalSizeBytes: totalTableBytes,
        totalSizePretty: this.formatBytes(totalTableBytes),
      },
      connections: {
        total: connectionItems.length,
        active: activeConnections,
        idle: idleConnections,
        idleInTransaction: idleInTransactionConnections,
        internal: internalConnections,
        other: otherConnections,
        items: connectionItems,
      },
      users: databaseUsers,
      indexes: indexStats,
      tables: {
        totalRows,
        totalSizeBytes: totalTableBytes,
        totalSizePretty: this.formatBytes(totalTableBytes),
        totalIndexes: runtime?.totalIndexes ?? indexStats.length,
        items: tableStats,
        topQueried: topQueriedTables,
      },
      backup: {
        format: 'application/x-tar',
        fileExtension: '.tar',
        provider: `google-drive-${this.getGoogleDriveProvider()}`,
      },
    };
  }

  private async insertBackupRecord(backup: {
    fileName: string;
    origin: BackupOrigin;
    sizeBytes: number;
    createdAt: Date;
  }) {
    await this.ensureRequiredTable(
      'database_backups',
      'No se pudo guardar el respaldo porque la tabla database_backups no existe. Ejecuta las migraciones de Prisma.',
    );

    const createdRows = await this.prisma.$queryRaw<BackupRecordRow[]>`
      INSERT INTO "management"."database_backups" ("fileName", "origin", "sizeBytes", "createdAt")
      VALUES (
        ${backup.fileName},
        CAST(${backup.origin} AS "public"."BackupOrigin"),
        ${backup.sizeBytes},
        ${backup.createdAt}
      )
      RETURNING "id", "fileName", "origin", "sizeBytes", "createdAt"
    `;

    const created = createdRows[0];
    if (!created) {
      throw new Error('No se pudo guardar el registro del respaldo');
    }

    return this.mapBackupRecordSummary(created);
  }

  private async deleteBackupRecordAssets(record: BackupRecordRow) {
    await this.ensureRequiredTable(
      'database_backups',
      'No se pudo eliminar el respaldo porque la tabla database_backups no existe. Ejecuta las migraciones de Prisma.',
    );

    const deletedRows = await this.prisma.$queryRaw<BackupRecordRow[]>`
      DELETE FROM "management"."database_backups"
      WHERE "id" = ${record.id}
      RETURNING "id", "fileName", "origin", "sizeBytes", "createdAt"
    `;

    const deleted = deletedRows[0];
    if (!deleted) {
      throw new NotFoundException('Respaldo no encontrado');
    }

    try {
      await this.deleteBackupFromDrive(deleted.fileName);
      return deleted;
    } catch (error) {
      if (this.shouldKeepBackupDeletionLocal(error)) {
        const message =
          error instanceof Error && error.message.trim()
            ? error.message.trim()
            : 'Error desconocido al eliminar en Google Drive';
        this.logger.warn(
          `Se elimino solo el registro local del respaldo ${deleted.fileName} porque Google Drive no esta disponible o requiere reautenticacion: ${message}`,
        );
        return deleted;
      }

      await this.restoreBackupRecord(deleted);
      throw error;
    }
  }

  private async restoreBackupRecord(record: BackupRecordRow) {
    await this.ensureRequiredTable(
      'database_backups',
      'No se pudo restaurar el registro del respaldo porque la tabla database_backups no existe. Ejecuta las migraciones de Prisma.',
    );

    await this.prisma.$queryRaw`
      INSERT INTO "management"."database_backups" ("id", "fileName", "origin", "sizeBytes", "createdAt")
      VALUES (
        ${record.id},
        ${record.fileName},
        CAST(${record.origin} AS "public"."BackupOrigin"),
        ${this.toSafeNumber(record.sizeBytes)},
        ${this.parseOptionalDate(record.createdAt) ?? new Date()}
      )
    `;
  }

  private async tryDeleteUploadedBackupSilently(fileName: string) {
    try {
      await this.deleteBackupFromDrive(fileName);
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : 'Error desconocido al revertir respaldo en Google Drive';
      this.logger.warn(
        `No se pudo revertir el archivo ${fileName} en Google Drive tras un error local: ${message}`,
      );
    }
  }

  private async listBackupTableNames() {
    const tableRows = await this.prisma.$queryRaw<
      Array<{
        tableName: string;
      }>
    >`
      SELECT (tables.table_schema || '.' || tables.table_name) AS "tableName"
      FROM information_schema.tables AS tables
      WHERE tables.table_schema IN ('accounts', 'catalog', 'management')
        AND tables.table_type = 'BASE TABLE'
        AND NOT (
          tables.table_schema = 'management'
          AND tables.table_name = '_prisma_migrations'
        )
      ORDER BY tables.table_schema ASC, tables.table_name ASC
    `;

    return tableRows.map((item) => item.tableName);
  }

  private async listBackupSchemaNames() {
    const rows = await this.prisma.$queryRaw<Array<{ schemaName: string }>>`
      SELECT DISTINCT tables.table_schema AS "schemaName"
      FROM information_schema.tables AS tables
      WHERE tables.table_schema IN ('accounts', 'catalog', 'management')
        AND tables.table_type = 'BASE TABLE'
        AND NOT (
          tables.table_schema = 'management'
          AND tables.table_name = '_prisma_migrations'
        )
      ORDER BY tables.table_schema ASC
    `;

    return rows.map((item) => item.schemaName);
  }

  private normalizeTableName(tableName: string) {
    const normalized = String(tableName ?? '')
      .trim()
      .toLowerCase();

    if (!normalized) {
      throw new BadRequestException(
        'Selecciona una tabla para generar el respaldo',
      );
    }

    if (!/^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)?$/.test(normalized)) {
      throw new BadRequestException('Nombre de tabla invalido');
    }

    return normalized;
  }

  private normalizeSchemaName(schemaName: string) {
    const normalized = String(schemaName ?? '')
      .trim()
      .toLowerCase();

    if (!normalized) {
      throw new BadRequestException(
        'Selecciona un esquema para generar el respaldo',
      );
    }

    if (
      !this.backupSchemas.includes(
        normalized as (typeof this.backupSchemas)[number],
      )
    ) {
      throw new BadRequestException('Nombre de esquema invalido');
    }

    return normalized;
  }

  private normalizeOptionalSchemaName(rawValue: unknown) {
    const normalized =
      this.normalizeOptionalText(rawValue)?.toLowerCase() ?? '';

    if (!normalized) {
      return null;
    }

    if (
      !this.backupSchemas.includes(
        normalized as (typeof this.backupSchemas)[number],
      )
    ) {
      throw new BadRequestException('Nombre de esquema invalido');
    }

    return normalized;
  }

  private createDefaultScheduleRow(): BackupScheduleRow {
    const now = new Date();
    return {
      id: this.scheduleRowId,
      isEnabled: this.scheduleDefaults.enabled,
      intervalDays: this.scheduleDefaults.everyDays,
      runAtTime: this.scheduleDefaults.runAtTime,
      retentionDays: this.scheduleDefaults.retentionDays,
      schemaName: null,
      lastRunAt: null,
      nextRunAt: null,
      createdAt: now,
      updatedAt: now,
    };
  }

  private async selectBackupScheduleRows() {
    const hasSchemaNameColumn = await this.hasBackupScheduleSchemaColumn();
    const schemaSelection = hasSchemaNameColumn
      ? `"schemaName"`
      : `NULL::text AS "schemaName"`;

    return this.prisma.$queryRawUnsafe(
      `
        SELECT
          "id",
          "isEnabled",
          "intervalDays",
          "runAtTime",
          "retentionDays",
          ${schemaSelection},
          "lastRunAt",
          "nextRunAt",
          "createdAt",
          "updatedAt"
        FROM "management"."database_backup_schedule"
        WHERE "id" = $1
        LIMIT 1
      `,
      this.scheduleRowId,
    );
  }

  private async upsertBackupScheduleRow(
    row: {
      id: number;
      isEnabled: boolean;
      intervalDays: number;
      runAtTime: string;
      retentionDays: number;
      schemaName: string | null;
      lastRunAt: Date | null;
      nextRunAt: Date | null;
      createdAt: Date;
      updatedAt: Date;
    },
    overwriteExisting: boolean,
  ) {
    const hasSchemaNameColumn = await this.hasBackupScheduleSchemaColumn();
    const schemaColumns = hasSchemaNameColumn ? `, "schemaName"` : '';
    const schemaValuePlaceholder = hasSchemaNameColumn ? `, $6` : '';
    const schemaUpdate = hasSchemaNameColumn
      ? `"schemaName" = EXCLUDED."schemaName",`
      : '';
    const schemaSelection = hasSchemaNameColumn
      ? `"schemaName"`
      : `NULL::text AS "schemaName"`;
    const conflictAction = overwriteExisting
      ? `
        DO UPDATE
        SET
          "isEnabled" = EXCLUDED."isEnabled",
          "intervalDays" = EXCLUDED."intervalDays",
          "runAtTime" = EXCLUDED."runAtTime",
          "retentionDays" = EXCLUDED."retentionDays",
          ${schemaUpdate}
          "lastRunAt" = EXCLUDED."lastRunAt",
          "nextRunAt" = EXCLUDED."nextRunAt",
          "updatedAt" = EXCLUDED."updatedAt"
      `
      : `DO NOTHING`;

    const params = hasSchemaNameColumn
      ? [
          row.id,
          row.isEnabled,
          row.intervalDays,
          row.runAtTime,
          row.retentionDays,
          row.schemaName,
          row.lastRunAt,
          row.nextRunAt,
          row.createdAt,
          row.updatedAt,
        ]
      : [
          row.id,
          row.isEnabled,
          row.intervalDays,
          row.runAtTime,
          row.retentionDays,
          row.lastRunAt,
          row.nextRunAt,
          row.createdAt,
          row.updatedAt,
        ];

    const sql = hasSchemaNameColumn
      ? `
        INSERT INTO "management"."database_backup_schedule" (
          "id",
          "isEnabled",
          "intervalDays",
          "runAtTime",
          "retentionDays"${schemaColumns},
          "lastRunAt",
          "nextRunAt",
          "createdAt",
          "updatedAt"
        )
        VALUES ($1, $2, $3, $4, $5${schemaValuePlaceholder}, $7, $8, $9, $10)
        ON CONFLICT ("id")
        ${conflictAction}
        RETURNING
          "id",
          "isEnabled",
          "intervalDays",
          "runAtTime",
          "retentionDays",
          ${schemaSelection},
          "lastRunAt",
          "nextRunAt",
          "createdAt",
          "updatedAt"
      `
      : `
        INSERT INTO "management"."database_backup_schedule" (
          "id",
          "isEnabled",
          "intervalDays",
          "runAtTime",
          "retentionDays",
          "lastRunAt",
          "nextRunAt",
          "createdAt",
          "updatedAt"
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT ("id")
        ${conflictAction}
        RETURNING
          "id",
          "isEnabled",
          "intervalDays",
          "runAtTime",
          "retentionDays",
          ${schemaSelection},
          "lastRunAt",
          "nextRunAt",
          "createdAt",
          "updatedAt"
      `;

    return this.prisma.$queryRawUnsafe(sql, ...params);
  }

  private async hasBackupScheduleSchemaColumn() {
    if (this.backupScheduleSchemaColumnExists !== null) {
      return this.backupScheduleSchemaColumnExists;
    }

    const rows = await this.prisma.$queryRaw<Array<{ exists: boolean }>>`
      SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'management'
          AND table_name = 'database_backup_schedule'
          AND column_name = 'schemaName'
      ) AS "exists"
    `;

    this.backupScheduleSchemaColumnExists = rows[0]?.exists === true;
    return this.backupScheduleSchemaColumnExists;
  }

  private async ensureRequiredTable(tableName: string, message: string) {
    if (await this.tableExists(tableName)) {
      return;
    }

    throw new InternalServerErrorException(message);
  }

  private async tableExists(tableName: string) {
    const rows = await this.prisma.$queryRaw<Array<{ exists: boolean }>>`
      SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ${this.managementSchema}
          AND table_name = ${tableName}
          AND table_type = 'BASE TABLE'
      ) AS "exists"
    `;

    return rows[0]?.exists === true;
  }

  private parseDatabaseUrl(): ParsedDatabaseUrl & { schema: string } {
    const rawDatabaseUrl =
      process.env.DATABASE_DIRECT_URL || process.env.DATABASE_URL;
    if (!rawDatabaseUrl) {
      throw new InternalServerErrorException(
        'DATABASE_URL no esta configurada',
      );
    }

    let parsed: URL;
    try {
      parsed = new URL(rawDatabaseUrl);
    } catch {
      throw new InternalServerErrorException(
        'DATABASE_URL tiene un formato invalido',
      );
    }

    if (!['postgresql:', 'postgres:'].includes(parsed.protocol)) {
      throw new InternalServerErrorException(
        'DATABASE_URL debe apuntar a PostgreSQL',
      );
    }

    const database = decodeURIComponent(parsed.pathname.replace(/^\/+/, ''));
    const schema = (parsed.searchParams.get('schema') ?? 'management').trim();

    if (!database) {
      throw new InternalServerErrorException(
        'DATABASE_URL no contiene el nombre de la base de datos',
      );
    }

    if (!this.isSafeIdentifier(schema)) {
      throw new InternalServerErrorException(
        'El schema de DATABASE_URL es invalido',
      );
    }

    const sslMode = this.normalizeSslMode(parsed.searchParams.get('sslmode'));
    const channelBinding = this.normalizeChannelBinding(
      parsed.searchParams.get('channel_binding'),
    );

    return {
      host: parsed.hostname || 'localhost',
      port: parsed.port || '5432',
      username: decodeURIComponent(parsed.username),
      password: decodeURIComponent(parsed.password),
      database,
      schema,
      sslMode,
      channelBinding,
    };
  }

  private resolvePgDumpCommand() {
    const configured = process.env.PG_DUMP_PATH?.trim();
    if (configured) {
      return configured;
    }

    return process.platform === 'win32' ? 'pg_dump.exe' : 'pg_dump';
  }

  private getConfiguredGoogleDriveProviders(): GoogleDriveProvider[] {
    const preferredProvider =
      process.env.GOOGLE_DRIVE_PROVIDER?.trim().toLowerCase();
    const providers: GoogleDriveProvider[] = [];

    const clientId = process.env.GOOGLE_OAUTH_CLIENT_ID?.trim();
    const clientSecret = process.env.GOOGLE_OAUTH_CLIENT_SECRET?.trim();
    const refreshToken = process.env.GOOGLE_OAUTH_REFRESH_TOKEN?.trim();
    if (clientId && clientSecret && refreshToken) {
      providers.push('oauth2');
    }

    const clientEmail = process.env.GOOGLE_DRIVE_CLIENT_EMAIL?.trim();
    const privateKey = process.env.GOOGLE_DRIVE_PRIVATE_KEY?.trim();
    if (clientEmail && privateKey) {
      providers.push('service-account');
    }

    if (
      preferredProvider &&
      preferredProvider !== 'oauth2' &&
      preferredProvider !== 'service-account'
    ) {
      throw new InternalServerErrorException(
        'Configura GOOGLE_DRIVE_PROVIDER con uno de estos valores: oauth2 o service-account',
      );
    }

    if (preferredProvider) {
      if (!providers.includes(preferredProvider as GoogleDriveProvider)) {
        throw new InternalServerErrorException(
          preferredProvider === 'oauth2'
            ? 'Configura GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET y GOOGLE_OAUTH_REFRESH_TOKEN para usar GOOGLE_DRIVE_PROVIDER=oauth2'
            : 'Configura GOOGLE_DRIVE_CLIENT_EMAIL y GOOGLE_DRIVE_PRIVATE_KEY para usar GOOGLE_DRIVE_PROVIDER=service-account',
        );
      }

      return [preferredProvider as GoogleDriveProvider];
    }

    if (providers.length > 0) {
      return providers;
    }

    throw new InternalServerErrorException(
      'Configura GOOGLE_DRIVE_CLIENT_EMAIL y GOOGLE_DRIVE_PRIVATE_KEY o bien GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET y GOOGLE_OAUTH_REFRESH_TOKEN para usar respaldos en Google Drive',
    );
  }

  private getGoogleDriveProvider(): GoogleDriveProvider {
    return this.getConfiguredGoogleDriveProviders()[0];
  }

  private createGoogleDriveClient(
    provider: GoogleDriveProvider = this.getGoogleDriveProvider(),
  ) {
    if (provider === 'service-account') {
      const clientEmail = process.env.GOOGLE_DRIVE_CLIENT_EMAIL?.trim();
      const rawPrivateKey = process.env.GOOGLE_DRIVE_PRIVATE_KEY?.trim();

      if (!clientEmail || !rawPrivateKey) {
        throw new InternalServerErrorException(
          'Configura GOOGLE_DRIVE_CLIENT_EMAIL y GOOGLE_DRIVE_PRIVATE_KEY para usar respaldos en Google Drive con service account',
        );
      }

      const auth = new google.auth.JWT({
        email: clientEmail,
        key: rawPrivateKey.replace(/\\n/g, '\n'),
        scopes: ['https://www.googleapis.com/auth/drive'],
      });

      return google.drive({
        version: 'v3',
        auth,
      });
    }

    const clientId = process.env.GOOGLE_OAUTH_CLIENT_ID?.trim();
    const clientSecret = process.env.GOOGLE_OAUTH_CLIENT_SECRET?.trim();
    const redirectUri =
      process.env.GOOGLE_OAUTH_REDIRECT_URI?.trim() ||
      'http://127.0.0.1:3005/oauth2callback';
    const refreshToken = process.env.GOOGLE_OAUTH_REFRESH_TOKEN?.trim();

    if (!clientId || !clientSecret || !refreshToken) {
      throw new InternalServerErrorException(
        'Configura GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET y GOOGLE_OAUTH_REFRESH_TOKEN para usar respaldos en Google Drive con OAuth 2.0',
      );
    }

    const auth = new google.auth.OAuth2(clientId, clientSecret, redirectUri);
    auth.setCredentials({
      refresh_token: refreshToken,
    });

    return google.drive({
      version: 'v3',
      auth,
    });
  }

  private async runWithGoogleDriveClient<T>(
    operationName: string,
    callback: (client: GoogleDriveClientContext) => Promise<T>,
  ) {
    const providers = this.getConfiguredGoogleDriveProviders();
    let lastError: unknown = null;

    for (let index = 0; index < providers.length; index += 1) {
      const provider = providers[index];

      try {
        return await callback({
          provider,
          drive: this.createGoogleDriveClient(provider),
        });
      } catch (error) {
        lastError = error;
        const hasNextProvider = index < providers.length - 1;

        if (
          hasNextProvider &&
          this.shouldRetryWithNextGoogleDriveProvider(error)
        ) {
          const detail =
            error instanceof Error && error.message.trim()
              ? error.message.trim()
              : 'Error desconocido';
          this.logger.warn(
            `Google Drive fallo con ${provider} al ${operationName}. Se intentara el siguiente proveedor configurado. Motivo: ${detail}`,
          );
          continue;
        }

        throw error;
      }
    }

    if (lastError instanceof Error) {
      throw lastError;
    }

    throw new InternalServerErrorException(
      `No se pudo ${operationName} en Google Drive`,
    );
  }

  private getGoogleDriveFolderId() {
    const folderId = process.env.GOOGLE_DRIVE_FOLDER_ID?.trim();
    if (!folderId) {
      throw new InternalServerErrorException(
        'Configura GOOGLE_DRIVE_FOLDER_ID para guardar respaldos en Google Drive',
      );
    }

    return folderId;
  }

  private async uploadBackupToDrive(fileName: string, content: Buffer) {
    const folderId = this.getGoogleDriveFolderId();

    try {
      await this.runWithGoogleDriveClient(
        'subir el respaldo',
        async ({ drive }) => {
          await drive.files.create({
            requestBody: {
              name: fileName,
              parents: [folderId],
            },
            media: {
              mimeType: 'application/x-tar',
              body: Readable.from(content),
            },
            supportsAllDrives: true,
          });
        },
      );
    } catch (error) {
      throw this.wrapGoogleDriveError(
        error,
        'No se pudo subir el respaldo a Google Drive',
      );
    }
  }

  private async downloadBackupFromDrive(fileName: string) {
    const driveFileId = await this.findDriveFileIdByName(fileName);
    if (!driveFileId) {
      throw new NotFoundException(
        `No se encontro el respaldo ${fileName} dentro de la carpeta de Google Drive configurada`,
      );
    }

    try {
      const response = await this.runWithGoogleDriveClient(
        `descargar el respaldo ${fileName}`,
        async ({ drive }) =>
          (await drive.files.get(
            {
              fileId: driveFileId,
              alt: 'media',
              supportsAllDrives: true,
            },
            {
              responseType: 'stream',
            },
          )) as { data: NodeJS.ReadableStream },
      );

      return await this.readStreamToBuffer(response.data);
    } catch (error) {
      const wrapped = this.wrapGoogleDriveError(
        error,
        `No se pudo descargar el respaldo ${fileName} desde Google Drive`,
      );

      if (this.isGoogleDriveFileMissing(error)) {
        throw new NotFoundException(
          'El archivo del respaldo ya no existe en Google Drive',
        );
      }

      throw wrapped;
    }
  }

  private async deleteBackupFromDrive(fileName: string) {
    const driveFileId = await this.findDriveFileIdByName(fileName, {
      allowMissing: true,
    });

    if (!driveFileId) {
      return;
    }

    try {
      await this.runWithGoogleDriveClient(
        'eliminar el respaldo',
        async ({ drive }) => {
          await drive.files.delete({
            fileId: driveFileId,
            supportsAllDrives: true,
          });
        },
      );
    } catch (error) {
      if (this.isGoogleDriveFileMissing(error)) {
        return;
      }

      throw this.wrapGoogleDriveError(
        error,
        'No se pudo eliminar el respaldo en Google Drive',
      );
    }
  }

  private async readStreamToBuffer(stream: NodeJS.ReadableStream) {
    return new Promise<Buffer>((resolve, reject) => {
      const chunks: Buffer[] = [];

      stream.on('data', (chunk: Buffer | string) => {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      });

      stream.on('end', () => {
        resolve(Buffer.concat(chunks));
      });

      stream.on('error', (error) => {
        const errorMessage =
          error instanceof Error
            ? error.message
            : 'Error desconocido al leer el archivo';
        reject(
          new InternalServerErrorException(
            `No se pudo leer el archivo de Google Drive: ${errorMessage}`,
          ),
        );
      });
    });
  }

  private toUnknownArray(value: unknown): unknown[] {
    return Array.isArray(value) ? value : [];
  }

  private getBackupScheduleRow(rows: unknown[]): BackupScheduleRow | null {
    if (rows.length === 0) {
      return null;
    }

    const candidate = rows[0];
    if (!candidate || typeof candidate !== 'object') {
      return null;
    }

    const row = candidate as Partial<BackupScheduleRow>;
    if (
      typeof row.id !== 'number' ||
      typeof row.isEnabled !== 'boolean' ||
      typeof row.runAtTime !== 'string'
    ) {
      return null;
    }

    return row as BackupScheduleRow;
  }

  private async findDriveFileIdByName(
    fileName: string,
    options?: { allowMissing?: boolean },
  ) {
    const folderId = this.getGoogleDriveFolderId();

    try {
      const response = await this.runWithGoogleDriveClient(
        `ubicar el respaldo ${fileName}`,
        async ({ drive }) =>
          drive.files.list({
            q: [
              `'${folderId}' in parents`,
              `name = '${this.escapeDriveQueryValue(fileName)}'`,
              'trashed = false',
            ].join(' and '),
            pageSize: 1,
            fields: 'files(id, name, createdTime)',
            orderBy: 'createdTime desc',
            includeItemsFromAllDrives: true,
            supportsAllDrives: true,
          }),
      );

      const fileId = response.data.files?.[0]?.id?.trim();
      if (fileId) {
        return fileId;
      }

      if (options?.allowMissing) {
        return null;
      }

      throw new NotFoundException(
        `No se encontro el respaldo ${fileName} dentro de la carpeta de Google Drive configurada`,
      );
    } catch (error) {
      if (error instanceof NotFoundException) {
        throw error;
      }

      throw this.wrapGoogleDriveError(
        error,
        `No se pudo ubicar el respaldo ${fileName} en Google Drive`,
      );
    }
  }

  private escapeDriveQueryValue(value: string) {
    return value.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
  }

  private normalizeOptionalText(value: unknown) {
    return typeof value === 'string' ? value.trim() : null;
  }

  private isGoogleDriveFileMissing(error: unknown) {
    return (
      error instanceof NotFoundException ||
      this.extractGoogleDriveStatus(error) === 404
    );
  }

  private shouldRetryWithNextGoogleDriveProvider(error: unknown) {
    const status = this.extractGoogleDriveStatus(error);
    if (status === 401 || status === 403) {
      return true;
    }

    const message = this.extractGoogleDriveErrorMessage(error);
    if (!message) {
      return false;
    }

    const normalized = message.toLowerCase();
    return (
      normalized.includes('invalid_grant') ||
      normalized.includes('invalid credentials') ||
      normalized.includes('unauthorized') ||
      normalized.includes('insufficient authentication') ||
      normalized.includes('token has been expired or revoked')
    );
  }

  private shouldKeepBackupDeletionLocal(error: unknown) {
    const status = this.extractGoogleDriveStatus(error);
    if (status === 401 || status === 403) {
      return true;
    }

    const message = this.extractGoogleDriveErrorMessage(error).toLowerCase();
    return (
      message.includes('invalid_grant') ||
      message.includes('invalid credentials') ||
      message.includes('unauthorized') ||
      message.includes('insufficient authentication') ||
      message.includes('token has been expired or revoked')
    );
  }

  private extractGoogleDriveStatus(error: unknown) {
    if (!error || typeof error !== 'object') {
      return undefined;
    }

    const candidate = error as {
      code?: number;
      status?: number;
      response?: { status?: number };
    };

    return candidate.code ?? candidate.status ?? candidate.response?.status;
  }

  private extractGoogleDriveErrorMessage(error: unknown) {
    if (error instanceof Error && error.message.trim()) {
      return error.message.trim();
    }

    if (!error || typeof error !== 'object') {
      return '';
    }

    const candidate = error as {
      response?: {
        data?: {
          error?: string | { message?: string };
          error_description?: string;
        };
      };
      errors?: Array<{ message?: string }>;
    };

    const responseError = candidate.response?.data?.error;
    if (typeof responseError === 'string' && responseError.trim()) {
      return responseError.trim();
    }

    const nestedMessage =
      typeof responseError === 'object' && responseError?.message?.trim()
        ? responseError.message.trim()
        : '';
    if (nestedMessage) {
      return nestedMessage;
    }

    const errorDescription =
      candidate.response?.data?.error_description?.trim();
    if (errorDescription) {
      return errorDescription;
    }

    const arrayMessage = candidate.errors?.[0]?.message?.trim();
    return arrayMessage || '';
  }

  private wrapGoogleDriveError(error: unknown, fallbackMessage: string) {
    if (error instanceof InternalServerErrorException) {
      return error;
    }

    if (error instanceof Error && error.message.trim()) {
      return new InternalServerErrorException(
        `${fallbackMessage}: ${error.message.trim()}`,
      );
    }

    return new InternalServerErrorException(fallbackMessage);
  }

  private isSafeIdentifier(value: string) {
    return /^[A-Za-z_][A-Za-z0-9_]*$/.test(value);
  }

  private parseQualifiedTableName(tableName: string) {
    const [schema, table] = tableName.split('.');

    if (!schema || !table) {
      throw new BadRequestException('Nombre de tabla invalido');
    }

    if (
      !this.backupSchemas.includes(
        schema as (typeof this.backupSchemas)[number],
      ) ||
      !this.isSafeIdentifier(table)
    ) {
      throw new BadRequestException('Nombre de tabla invalido');
    }

    return { schema, table };
  }

  private normalizeSslMode(rawValue: string | null) {
    if (!rawValue) {
      return undefined;
    }

    const value = rawValue.trim().toLowerCase();
    const validModes = new Set([
      'disable',
      'allow',
      'prefer',
      'require',
      'verify-ca',
      'verify-full',
    ]);
    return validModes.has(value) ? value : undefined;
  }

  private normalizeChannelBinding(rawValue: string | null) {
    if (!rawValue) {
      return undefined;
    }

    const value = rawValue.trim().toLowerCase();
    const validModes = new Set(['disable', 'prefer', 'require']);
    return validModes.has(value) ? value : undefined;
  }

  private async runPgDump(
    tableName?: string,
    schemaName?: string,
  ): Promise<PgDumpResult> {
    const databaseConfig = this.parseDatabaseUrl();
    const pgDumpCommand = this.resolvePgDumpCommand();
    const args = [
      '--format=tar',
      '--no-owner',
      '--no-privileges',
      '--encoding=UTF8',
      '--blobs',
      '--verbose',
      '--host',
      databaseConfig.host,
      '--port',
      databaseConfig.port,
      '--dbname',
      databaseConfig.database,
    ];

    if (databaseConfig.username) {
      args.push('--username', databaseConfig.username);
    }

    if (tableName) {
      const target = this.parseQualifiedTableName(tableName);
      args.push('--table', `${target.schema}.${target.table}`);
    } else if (schemaName) {
      const normalizedSchemaName = this.normalizeSchemaName(schemaName);
      args.push('--schema', normalizedSchemaName);
    }

    return new Promise<PgDumpResult>((resolve, reject) => {
      const stdoutChunks: Buffer[] = [];
      const stderrChunks: Buffer[] = [];

      const dumpProcess = spawn(pgDumpCommand, args, {
        env: {
          ...process.env,
          PGPASSWORD: databaseConfig.password,
          ...(databaseConfig.sslMode
            ? { PGSSLMODE: databaseConfig.sslMode }
            : {}),
          ...(databaseConfig.channelBinding
            ? { PGCHANNELBINDING: databaseConfig.channelBinding }
            : {}),
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      });

      dumpProcess.stdout.on('data', (chunk: Buffer | string) => {
        stdoutChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      });

      dumpProcess.stderr.on('data', (chunk: Buffer | string) => {
        stderrChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      });

      dumpProcess.on('error', (error: NodeJS.ErrnoException) => {
        if (error.code === 'ENOENT') {
          reject(
            new InternalServerErrorException(
              'No se encontro pg_dump. Configura PG_DUMP_PATH o agrega PostgreSQL al PATH del servidor.',
            ),
          );
          return;
        }

        reject(
          new InternalServerErrorException(
            `No se pudo iniciar pg_dump: ${error.message}`,
          ),
        );
      });

      dumpProcess.on('close', (code) => {
        if (code === 0) {
          const output = Buffer.concat(stdoutChunks);
          if (output.length > 0) {
            resolve({
              content: output,
              verboseLog: this.normalizeProcessLog(
                Buffer.concat(stderrChunks).toString('utf8'),
              ),
            });
            return;
          }

          reject(
            new InternalServerErrorException(
              'pg_dump finalizo sin contenido de respaldo',
            ),
          );
          return;
        }

        const stderrMessage = Buffer.concat(stderrChunks)
          .toString('utf8')
          .trim();
        reject(
          new InternalServerErrorException(
            stderrMessage ||
              `pg_dump finalizo con codigo ${code ?? 'desconocido'}`,
          ),
        );
      });
    });
  }

  private composeBackupExecutionLog({
    backupType,
    tableName,
    schemaName,
    fileName,
    sizeBytes,
    requestedAt,
    backupCreatedAt,
    provider,
    verboseLog,
    recordId,
  }: {
    backupType: 'database' | 'schema' | 'table';
    tableName?: string;
    schemaName?: string;
    fileName: string;
    sizeBytes: number;
    requestedAt: Date;
    backupCreatedAt: string;
    provider: GoogleDriveProvider;
    verboseLog: string;
    recordId: number;
  }) {
    const lines = [
      '===== INICIO BACKUP =====',
      `Fecha: ${this.formatLogDate(requestedAt)}`,
      `${this.formatLogStamp(requestedAt)}  Solicitud recibida para ${
        backupType === 'table' && tableName
          ? `la tabla ${tableName}`
          : backupType === 'schema' && schemaName
            ? `el esquema ${schemaName}`
            : 'respaldo completo de la base de datos'
      }`,
      `${this.formatLogStamp(requestedAt)}  Nombre de archivo previsto: ${fileName}`,
      `${this.formatLogStamp(requestedAt)}  Ejecutando pg_dump en modo verbose...`,
    ];

    if (verboseLog) {
      lines.push(verboseLog);
    }

    const finishedAt = this.parseOptionalDate(backupCreatedAt) ?? new Date();
    lines.push(
      `${this.formatLogStamp(finishedAt)}  Archivo generado: ${fileName}`,
      `${this.formatLogStamp(finishedAt)}  Tamano final: ${this.formatBytes(sizeBytes)}`,
      `${this.formatLogStamp(finishedAt)}  Respaldo enviado a Google Drive (${provider})`,
      `${this.formatLogStamp(finishedAt)}  Registro guardado en historial local con ID ${recordId}`,
      'RESULTADO: BACKUP EXITOSO',
      '===== FIN BACKUP =====',
    );

    return lines.join('\n');
  }

  private normalizeProcessLog(value: string) {
    return value
      .replace(/\r\n/g, '\n')
      .split('\n')
      .map((line) => line.trimEnd())
      .filter(
        (line, index, all) =>
          line.length > 0 || (index > 0 && all[index - 1] !== ''),
      )
      .join('\n')
      .trim();
  }

  private formatLogDate(value: Date) {
    return value.toLocaleString('es-MX', {
      hour12: true,
    });
  }

  private formatLogStamp(value: Date) {
    return value.toISOString();
  }

  private parseOptionalDate(value: Date | string | null | undefined) {
    if (!value) {
      return null;
    }

    const parsed = value instanceof Date ? value : new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  private toIsoString(value: Date | string | null | undefined) {
    const parsed = this.parseOptionalDate(value);
    return parsed ? parsed.toISOString() : null;
  }

  private toSafeNumber(value: bigint | number | string | undefined) {
    if (typeof value === 'number' && Number.isFinite(value)) {
      return value;
    }

    if (typeof value === 'bigint') {
      return Number(value);
    }

    if (typeof value === 'string') {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : 0;
    }

    return 0;
  }

  private formatBytes(bytes: number) {
    if (!Number.isFinite(bytes) || bytes <= 0) {
      return '0 B';
    }

    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const exponent = Math.min(
      Math.floor(Math.log(bytes) / Math.log(1024)),
      units.length - 1,
    );
    const value = bytes / 1024 ** exponent;
    return `${value.toFixed(exponent === 0 ? 0 : 2)} ${units[exponent]}`;
  }
}
