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
  sizeBytes: number;
  createdAt: string;
};

type BackupRecordRow = {
  id: number;
  fileName: string;
  sizeBytes: number | bigint | string;
  createdAt: Date | string;
};

type BackupScheduleSummary = {
  enabled: boolean;
  everyDays: number;
  runAtTime: string;
  retentionDays: number;
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

@Injectable()
export class BackupsService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(BackupsService.name);
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
    return this.createAndStoreDatabaseBackupRecord();
  }

  async createSingleTableBackupRecord(tableName: string) {
    return this.createAndStoreSingleTableBackupRecord(tableName);
  }

  async listDatabaseBackupRecords() {
    const rows = await this.prisma.$queryRaw<
      Array<{
        id: number;
        fileName: string;
        sizeBytes: number | bigint | string;
        createdAt: Date | string;
      }>
    >`
      SELECT "id", "fileName", "sizeBytes", "createdAt"
      FROM "database_backups"
      ORDER BY "createdAt" DESC, "id" DESC
    `;

    return rows.map((item) => this.mapBackupRecordSummary(item));
  }

  async getDatabaseBackupSchedule() {
    const row = await this.getOrCreateBackupScheduleRow();
    return this.mapBackupScheduleSummary(row);
  }

  async updateDatabaseBackupSchedule(payload: Record<string, unknown>) {
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

    const updatedRows: unknown = await this.prisma.$queryRaw<unknown[]>`
      INSERT INTO "database_backup_schedule" (
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
      VALUES (
        ${this.scheduleRowId},
        ${normalized.enabled},
        ${normalized.everyDays},
        ${normalized.runAtTime},
        ${normalized.retentionDays},
        ${this.parseOptionalDate(existing.lastRunAt)},
        ${nextRunAt},
        ${this.parseOptionalDate(existing.createdAt) ?? now},
        ${now}
      )
      ON CONFLICT ("id") DO UPDATE
      SET
        "isEnabled" = EXCLUDED."isEnabled",
        "intervalDays" = EXCLUDED."intervalDays",
        "runAtTime" = EXCLUDED."runAtTime",
        "retentionDays" = EXCLUDED."retentionDays",
        "lastRunAt" = EXCLUDED."lastRunAt",
        "nextRunAt" = EXCLUDED."nextRunAt",
        "updatedAt" = EXCLUDED."updatedAt"
      RETURNING
        "id",
        "isEnabled",
        "intervalDays",
        "runAtTime",
        "retentionDays",
        "lastRunAt",
        "nextRunAt",
        "createdAt",
        "updatedAt"
    `;

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
    const existing = await this.getOrCreateBackupScheduleRow();
    const now = new Date();

    const resetRows: unknown = await this.prisma.$queryRaw<unknown[]>`
      INSERT INTO "database_backup_schedule" (
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
      VALUES (
        ${this.scheduleRowId},
        ${this.scheduleDefaults.enabled},
        ${this.scheduleDefaults.everyDays},
        ${this.scheduleDefaults.runAtTime},
        ${this.scheduleDefaults.retentionDays},
        ${null},
        ${null},
        ${this.parseOptionalDate(existing.createdAt) ?? now},
        ${now}
      )
      ON CONFLICT ("id") DO UPDATE
      SET
        "isEnabled" = EXCLUDED."isEnabled",
        "intervalDays" = EXCLUDED."intervalDays",
        "runAtTime" = EXCLUDED."runAtTime",
        "retentionDays" = EXCLUDED."retentionDays",
        "lastRunAt" = EXCLUDED."lastRunAt",
        "nextRunAt" = EXCLUDED."nextRunAt",
        "updatedAt" = EXCLUDED."updatedAt"
      RETURNING
        "id",
        "isEnabled",
        "intervalDays",
        "runAtTime",
        "retentionDays",
        "lastRunAt",
        "nextRunAt",
        "createdAt",
        "updatedAt"
    `;

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
    const rows = await this.prisma.$queryRaw<BackupRecordRow[]>`
      SELECT "id", "fileName", "sizeBytes", "createdAt"
      FROM "database_backups"
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
    const existingRows = await this.prisma.$queryRaw<BackupRecordRow[]>`
      SELECT "id", "fileName", "sizeBytes", "createdAt"
      FROM "database_backups"
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

  private async createAndStoreDatabaseBackupRecord() {
    const backup = await this.buildDatabaseBackupPayload();
    let record: BackupRecordSummary;

    try {
      record = await this.insertBackupRecord(backup);
    } catch (error) {
      await this.tryDeleteUploadedBackupSilently(backup.fileName);
      throw error;
    }

    await this.applyRetentionPolicy();
    return record;
  }

  private async createAndStoreSingleTableBackupRecord(tableName: string) {
    const backup = await this.buildSingleTableBackupPayload(tableName);
    let record: BackupRecordSummary;

    try {
      record = await this.insertBackupRecord(backup);
    } catch (error) {
      await this.tryDeleteUploadedBackupSilently(backup.fileName);
      throw error;
    }

    await this.applyRetentionPolicy();
    return record;
  }

  private async runAutomationTick() {
    if (this.schedulerInProgress) {
      return;
    }

    this.schedulerInProgress = true;

    try {
      const schedule = await this.getOrCreateBackupScheduleRow();
      const now = new Date();

      await this.ensurePersistedNextRunAt(schedule, now);

      if (schedule.isEnabled) {
        const dueAt = this.parseOptionalDate(schedule.nextRunAt);
        if (dueAt && dueAt.getTime() <= now.getTime()) {
          this.logger.log(
            `Iniciando respaldo automatico programado para ${schedule.runAtTime} cada ${this.toSafeNumber(schedule.intervalDays)} dia(s)`,
          );

          const createdBackup = await this.createAndStoreDatabaseBackupRecord();
          const executedAt = new Date(createdBackup.createdAt);
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
            `Respaldo automatico completado: ${createdBackup.fileName}`,
          );
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
      UPDATE "database_backup_schedule"
      SET
        "lastRunAt" = ${lastRunAt},
        "nextRunAt" = ${nextRunAt},
        "updatedAt" = ${new Date()}
      WHERE "id" = ${id}
    `;
  }

  private async getOrCreateBackupScheduleRow(): Promise<BackupScheduleRow> {
    const existingRows: unknown = await this.prisma.$queryRaw<unknown[]>`
      SELECT
        "id",
        "isEnabled",
        "intervalDays",
        "runAtTime",
        "retentionDays",
        "lastRunAt",
        "nextRunAt",
        "createdAt",
        "updatedAt"
      FROM "database_backup_schedule"
      WHERE "id" = ${this.scheduleRowId}
      LIMIT 1
    `;

    const existing = this.getBackupScheduleRow(
      this.toUnknownArray(existingRows),
    );
    if (existing) {
      return existing;
    }

    const now = new Date();
    const createdRows: unknown = await this.prisma.$queryRaw<unknown[]>`
      INSERT INTO "database_backup_schedule" (
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
      VALUES (
        ${this.scheduleRowId},
        ${this.scheduleDefaults.enabled},
        ${this.scheduleDefaults.everyDays},
        ${this.scheduleDefaults.runAtTime},
        ${this.scheduleDefaults.retentionDays},
        ${null},
        ${null},
        ${now},
        ${now}
      )
      ON CONFLICT ("id") DO NOTHING
      RETURNING
        "id",
        "isEnabled",
        "intervalDays",
        "runAtTime",
        "retentionDays",
        "lastRunAt",
        "nextRunAt",
        "createdAt",
        "updatedAt"
    `;

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
      lastRunAt: this.toIsoString(row.lastRunAt),
      nextRunAt: this.toIsoString(row.nextRunAt),
      createdAt: this.toIsoString(row.createdAt) ?? new Date().toISOString(),
      updatedAt: this.toIsoString(row.updatedAt) ?? new Date().toISOString(),
    };
  }

  private normalizeSchedulePayload(payload: Record<string, unknown>) {
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
      SELECT "id", "fileName", "sizeBytes", "createdAt"
      FROM "database_backups"
      WHERE "createdAt" < ${cutoffDate}
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
    const content = await this.runPgDump();
    await this.uploadBackupToDrive(fileName, content);

    return {
      fileName,
      sizeBytes: content.length,
      createdAt: now,
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
    const content = await this.runPgDump(tableName);
    await this.uploadBackupToDrive(fileName, content);

    return {
      fileName,
      sizeBytes: content.length,
      createdAt: now,
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
    sizeBytes: number | bigint | string;
    createdAt: Date | string;
  }): BackupRecordSummary {
    return {
      id: item.id,
      fileName: item.fileName,
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
    return `cemydi_${tableName}_backup_${timestamp.date}_${timestamp.time}.tar`;
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
    const [runtimeRows, tableRows] = await Promise.all([
      this.prisma.$queryRaw<
        Array<{
          databaseName: string;
          version: string;
          sizeBytes: bigint | number | string;
          activeConnections: number;
          idleConnections: number;
          totalConnections: number;
          commits: bigint | number | string;
          rollbacks: bigint | number | string;
          uptimeSeconds: bigint | number | string;
        }>
      >`
        SELECT
          current_database() AS "databaseName",
          version() AS "version",
          pg_database_size(current_database()) AS "sizeBytes",
          (
            SELECT COUNT(*)::int
            FROM pg_stat_activity
            WHERE datname = current_database() AND state = 'active'
          ) AS "activeConnections",
          (
            SELECT COUNT(*)::int
            FROM pg_stat_activity
            WHERE datname = current_database() AND state = 'idle'
          ) AS "idleConnections",
          (
            SELECT COUNT(*)::int
            FROM pg_stat_activity
            WHERE datname = current_database()
          ) AS "totalConnections",
          (
            SELECT xact_commit
            FROM pg_stat_database
            WHERE datname = current_database()
          ) AS "commits",
          (
            SELECT xact_rollback
            FROM pg_stat_database
            WHERE datname = current_database()
          ) AS "rollbacks",
          EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))::bigint AS "uptimeSeconds"
      `,
      this.prisma.$queryRaw<
        Array<{
          tableName: string;
          rowCount: bigint | number | string;
          sizeBytes: bigint | number | string;
        }>
      >`
        SELECT
          table_stats."tableName",
          COALESCE(table_stats."rowCount", 0) AS "rowCount",
          COALESCE(table_stats."sizeBytes", 0) AS "sizeBytes"
        FROM (
          SELECT
            tables.table_name AS "tableName",
            (
              xpath(
                '/row/count/text()',
                query_to_xml(
                  format(
                    'SELECT COUNT(*)::bigint AS count FROM %I.%I',
                    tables.table_schema,
                    tables.table_name
                  ),
                  true,
                  true,
                  ''
                )
              )
            )[1]::text::bigint AS "rowCount",
            pg_total_relation_size(
              to_regclass(format('%I.%I', tables.table_schema, tables.table_name))
            )::bigint AS "sizeBytes"
          FROM information_schema.tables AS tables
          WHERE tables.table_schema = 'public'
            AND tables.table_type = 'BASE TABLE'
            AND tables.table_name <> '_prisma_migrations'
        ) AS table_stats
        ORDER BY table_stats."sizeBytes" DESC, table_stats."tableName" ASC
      `,
    ]);

    const runtime = runtimeRows[0];
    const sizeBytes = this.toSafeNumber(runtime?.sizeBytes);
    const commits = this.toSafeNumber(runtime?.commits);
    const rollbacks = this.toSafeNumber(runtime?.rollbacks);
    const uptimeSeconds = this.toSafeNumber(runtime?.uptimeSeconds);
    const tableStats = tableRows.map((item) => {
      const tableSizeBytes = this.toSafeNumber(item.sizeBytes);
      return {
        tableName: item.tableName,
        rowCount: this.toSafeNumber(item.rowCount),
        sizeBytes: tableSizeBytes,
        sizePretty: this.formatBytes(tableSizeBytes),
      };
    });

    const totalRows = tableStats.reduce((acc, item) => acc + item.rowCount, 0);
    const totalTableBytes = tableStats.reduce(
      (acc, item) => acc + item.sizeBytes,
      0,
    );

    return {
      checkedAt: new Date().toISOString(),
      isOnline: true,
      databaseName: runtime?.databaseName ?? 'unknown',
      dbVersion: runtime?.version ?? 'unknown',
      uptimeSeconds,
      sizeBytes,
      sizePretty: this.formatBytes(sizeBytes),
      connections: {
        total: runtime?.totalConnections ?? 0,
        active: runtime?.activeConnections ?? 0,
        idle: runtime?.idleConnections ?? 0,
      },
      transactions: {
        commits,
        rollbacks,
      },
      tables: {
        totalRows,
        totalSizeBytes: totalTableBytes,
        totalSizePretty: this.formatBytes(totalTableBytes),
        items: tableStats,
      },
      backup: {
        format: 'application/x-tar',
        fileExtension: '.tar',
        provider: 'google-drive-oauth2',
      },
    };
  }

  private async insertBackupRecord(backup: {
    fileName: string;
    sizeBytes: number;
    createdAt: Date;
  }) {
    const createdRows = await this.prisma.$queryRaw<BackupRecordRow[]>`
      INSERT INTO "database_backups" ("fileName", "sizeBytes", "createdAt")
      VALUES (${backup.fileName}, ${backup.sizeBytes}, ${backup.createdAt})
      RETURNING "id", "fileName", "sizeBytes", "createdAt"
    `;

    const created = createdRows[0];
    if (!created) {
      throw new Error('No se pudo guardar el registro del respaldo');
    }

    return this.mapBackupRecordSummary(created);
  }

  private async deleteBackupRecordAssets(record: BackupRecordRow) {
    const deletedRows = await this.prisma.$queryRaw<BackupRecordRow[]>`
      DELETE FROM "database_backups"
      WHERE "id" = ${record.id}
      RETURNING "id", "fileName", "sizeBytes", "createdAt"
    `;

    const deleted = deletedRows[0];
    if (!deleted) {
      throw new NotFoundException('Respaldo no encontrado');
    }

    try {
      await this.deleteBackupFromDrive(deleted.fileName);
      return deleted;
    } catch (error) {
      await this.restoreBackupRecord(deleted);
      throw error;
    }
  }

  private async restoreBackupRecord(record: BackupRecordRow) {
    await this.prisma.$queryRaw`
      INSERT INTO "database_backups" ("id", "fileName", "sizeBytes", "createdAt")
      VALUES (${record.id}, ${record.fileName}, ${this.toSafeNumber(record.sizeBytes)}, ${this.parseOptionalDate(record.createdAt) ?? new Date()})
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
      SELECT tables.table_name AS "tableName"
      FROM information_schema.tables AS tables
      WHERE tables.table_schema = 'public'
        AND tables.table_type = 'BASE TABLE'
        AND tables.table_name <> '_prisma_migrations'
      ORDER BY tables.table_name ASC
    `;

    return tableRows.map((item) => item.tableName);
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

    if (!/^[a-z][a-z0-9_]*$/.test(normalized)) {
      throw new BadRequestException('Nombre de tabla invalido');
    }

    return normalized;
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
    const schema = (parsed.searchParams.get('schema') ?? 'public').trim();

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

  private createGoogleDriveClient() {
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
    const drive = this.createGoogleDriveClient();
    const folderId = this.getGoogleDriveFolderId();

    try {
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
    } catch (error) {
      throw this.wrapGoogleDriveError(
        error,
        'No se pudo subir el respaldo a Google Drive',
      );
    }
  }

  private async downloadBackupFromDrive(fileName: string) {
    const drive = this.createGoogleDriveClient();
    const driveFileId = await this.findDriveFileIdByName(fileName);
    if (!driveFileId) {
      throw new NotFoundException(
        `No se encontro el respaldo ${fileName} dentro de la carpeta de Google Drive configurada`,
      );
    }

    try {
      const response = (await drive.files.get(
        {
          fileId: driveFileId,
          alt: 'media',
          supportsAllDrives: true,
        },
        {
          responseType: 'stream',
        },
      )) as { data: NodeJS.ReadableStream };

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
    const drive = this.createGoogleDriveClient();
    const driveFileId = await this.findDriveFileIdByName(fileName, {
      allowMissing: true,
    });

    if (!driveFileId) {
      return;
    }

    try {
      await drive.files.delete({
        fileId: driveFileId,
        supportsAllDrives: true,
      });
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
    const drive = this.createGoogleDriveClient();
    const folderId = this.getGoogleDriveFolderId();

    try {
      const response = await drive.files.list({
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
      });

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

  private isGoogleDriveFileMissing(error: unknown) {
    return (
      error instanceof NotFoundException ||
      this.extractGoogleDriveStatus(error) === 404
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

  private async runPgDump(tableName?: string): Promise<Buffer> {
    const databaseConfig = this.parseDatabaseUrl();
    const pgDumpCommand = this.resolvePgDumpCommand();
    const args = [
      '--format=tar',
      '--no-owner',
      '--no-privileges',
      '--encoding=UTF8',
      '--blobs',
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
      args.push('--table', `${databaseConfig.schema}.${tableName}`);
    }

    return new Promise<Buffer>((resolve, reject) => {
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
            resolve(output);
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
