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
import { PrismaService } from '../../prisma/prisma.service';

type MaintenanceOperation = 'VACUUM' | 'ANALYZE' | 'VACUUM_ANALYZE';

type MaintenanceRunResult = {
  operation: MaintenanceOperation;
  schemaName: string | null;
  tableName: string | null;
  processedTables: number;
  startedAt: string;
  finishedAt: string;
  logText: string;
  message: string;
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

type MaintenanceScheduleSummary = {
  enabled: boolean;
  everyDays: number;
  runAtTime: string;
  operation: MaintenanceOperation;
  schemaName: string | null;
  tableName: string | null;
  lastRunAt: string | null;
  nextRunAt: string | null;
  createdAt: string;
  updatedAt: string;
};

type MaintenanceScheduleRow = {
  id: number;
  isEnabled: boolean;
  intervalDays: number | bigint | string;
  runAtTime: string;
  operation: MaintenanceOperation;
  schemaName: string | null;
  tableName: string | null;
  lastRunAt: Date | string | null;
  nextRunAt: Date | string | null;
  createdAt: Date | string;
  updatedAt: Date | string;
};

type PsqlExecutionResult = {
  verboseLog: string;
};

@Injectable()
export class MaintenanceService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(MaintenanceService.name);
  private readonly managementSchema = 'management';
  private readonly maintenanceSchemas = [
    'accounts',
    'catalog',
    'management',
  ] as const;
  private readonly scheduleDefaults = {
    enabled: false,
    everyDays: 1,
    runAtTime: '04:00',
    operation: 'VACUUM_ANALYZE' as MaintenanceOperation,
    tableName: null as string | null,
  } as const;
  private readonly scheduleRowId = 1;
  private readonly schedulerPollMs = 60_000;

  private schedulerTimer: NodeJS.Timeout | null = null;
  private schedulerInProgress = false;
  private maintenanceScheduleSchemaColumnExists: boolean | null = null;

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

  async run(payload: Record<string, unknown>): Promise<MaintenanceRunResult> {
    return this.executeMaintenance(payload);
  }

  async getSchedule() {
    if (!(await this.tableExists('database_maintenance_schedule'))) {
      return this.mapScheduleSummary(this.createDefaultScheduleRow());
    }

    return this.mapScheduleSummary(await this.getOrCreateScheduleRow());
  }

  async updateSchedule(payload: Record<string, unknown>) {
    await this.ensureRequiredTable(
      'database_maintenance_schedule',
      'No se pudo guardar la programacion de mantenimiento porque la tabla database_maintenance_schedule no existe. Ejecuta la migracion o crea la tabla manualmente.',
    );
    const existing = await this.getOrCreateScheduleRow();
    const normalized = await this.normalizeSchedulePayload(payload);
    const nextRunAt = normalized.enabled
      ? this.computeNextRunAt(
          new Date(),
          normalized.everyDays,
          normalized.runAtTime,
          this.parseOptionalDate(existing.lastRunAt),
        )
      : null;
    const now = new Date();
    const updatedRows = await this.upsertMaintenanceScheduleRow(
      {
        id: this.scheduleRowId,
        isEnabled: normalized.enabled,
        intervalDays: normalized.everyDays,
        runAtTime: normalized.runAtTime,
        operation: normalized.operation,
        schemaName: normalized.schemaName,
        tableName: normalized.tableName,
        lastRunAt: this.parseOptionalDate(existing.lastRunAt),
        nextRunAt,
        createdAt: this.parseOptionalDate(existing.createdAt) ?? now,
        updatedAt: now,
      },
      true,
    );

    const updated = this.getScheduleRow(this.toUnknownArray(updatedRows));
    if (!updated) {
      throw new InternalServerErrorException(
        'No se pudo guardar la programacion automatica de mantenimiento',
      );
    }

    return this.mapScheduleSummary(updated);
  }

  async deleteSchedule() {
    await this.ensureRequiredTable(
      'database_maintenance_schedule',
      'No se pudo eliminar la programacion de mantenimiento porque la tabla database_maintenance_schedule no existe. Ejecuta la migracion o crea la tabla manualmente.',
    );
    const existing = await this.getOrCreateScheduleRow();
    const now = new Date();
    const resetRows = await this.upsertMaintenanceScheduleRow(
      {
        id: this.scheduleRowId,
        isEnabled: false,
        intervalDays: this.scheduleDefaults.everyDays,
        runAtTime: this.scheduleDefaults.runAtTime,
        operation: this.scheduleDefaults.operation,
        schemaName: null,
        tableName: null,
        lastRunAt: this.parseOptionalDate(existing.lastRunAt),
        nextRunAt: null,
        createdAt: this.parseOptionalDate(existing.createdAt) ?? now,
        updatedAt: now,
      },
      true,
    );

    const reset = this.getScheduleRow(this.toUnknownArray(resetRows));
    if (!reset) {
      throw new InternalServerErrorException(
        'No se pudo eliminar la programacion automatica de mantenimiento',
      );
    }

    return this.mapScheduleSummary(reset);
  }

  private async executeMaintenance(
    payload: Record<string, unknown>,
  ): Promise<MaintenanceRunResult> {
    const operation = this.normalizeOperation(payload.operation);
    const requestedTableName = await this.normalizeOptionalTableName(
      payload.tableName,
    );
    const requestedSchemaName = this.normalizeRequestedSchemaName(
      payload.schemaName,
      requestedTableName,
    );
    const startedAt = new Date();
    const logs = [
      '===== INICIO MANTENIMIENTO =====',
      `Fecha: ${this.formatLogDate(startedAt)}`,
      `${this.formatLogStamp(startedAt)}  Operacion solicitada: ${this.formatOperationLabel(operation)}`,
    ];

    const tables = requestedTableName
      ? [requestedTableName]
      : await this.listMaintenanceTableNames(requestedSchemaName);

    if (tables.length === 0) {
      throw new NotFoundException(
        'No se encontraron tablas disponibles para ejecutar mantenimiento',
      );
    }

    logs.push(
      `${this.formatLogStamp(startedAt)}  Alcance: ${
        requestedTableName
          ? `tabla ${requestedTableName}`
          : requestedSchemaName
            ? `esquema ${requestedSchemaName} (${tables.length} tabla(s))`
            : `base de datos completa (${tables.length} tabla(s))`
      }`,
      `${this.formatLogStamp(startedAt)}  Iniciando ${this.formatOperationLabel(operation)}...`,
    );

    try {
      let processedTables = 0;

      for (const [index, tableName] of tables.entries()) {
        const stepStart = new Date();
        logs.push(
          `${this.formatLogStamp(stepStart)}  Procesando tabla ${tableName} (${index + 1}/${tables.length})`,
        );

        const execution = await this.runPsqlCommand(operation, tableName);
        if (execution.verboseLog) {
          logs.push(execution.verboseLog);
        }

        processedTables += 1;
        logs.push(
          `${this.formatLogStamp(new Date())}  Tabla ${tableName} procesada correctamente`,
        );
      }

      const finishedAt = new Date();
      logs.push(
        `${this.formatLogStamp(finishedAt)}  Finalizado correctamente`,
        'RESULTADO: MANTENIMIENTO EXITOSO',
        '===== FIN MANTENIMIENTO =====',
      );

      return {
        operation,
        schemaName: requestedSchemaName,
        tableName: requestedTableName,
        processedTables,
        startedAt: startedAt.toISOString(),
        finishedAt: finishedAt.toISOString(),
        logText: this.normalizeProcessLog(logs.join('\n')),
        message: `${this.formatOperationLabel(operation)} ejecutado correctamente`,
      };
    } catch (error) {
      const finishedAt = new Date();
      const message =
        error instanceof Error && error.message.trim()
          ? error.message.trim()
          : 'Error desconocido durante el mantenimiento';
      logs.push(
        `${this.formatLogStamp(finishedAt)}  Error detectado: ${message}`,
        'RESULTADO: MANTENIMIENTO FALLIDO',
        '===== FIN MANTENIMIENTO =====',
      );

      throw new InternalServerErrorException({
        message: `No se pudo ejecutar ${this.formatOperationLabel(operation)}`,
        logText: this.normalizeProcessLog(logs.join('\n')),
        operation,
        schemaName: requestedSchemaName,
        tableName: requestedTableName,
        startedAt: startedAt.toISOString(),
        finishedAt: finishedAt.toISOString(),
      });
    }
  }

  private async runAutomationTick() {
    if (this.schedulerInProgress) {
      return;
    }

    this.schedulerInProgress = true;

    try {
      if (!(await this.tableExists('database_maintenance_schedule'))) {
        return;
      }

      const schedule = await this.getOrCreateScheduleRow();
      const now = new Date();

      await this.ensurePersistedNextRunAt(schedule, now);

      if (!schedule.isEnabled) {
        return;
      }

      const dueAt = this.parseOptionalDate(schedule.nextRunAt);
      if (!dueAt || dueAt.getTime() > now.getTime()) {
        return;
      }

      this.logger.log(
        `Iniciando mantenimiento automatico ${this.formatOperationLabel(schedule.operation)} para ${
          schedule.tableName
            ? schedule.tableName
            : schedule.schemaName
              ? `el esquema ${schedule.schemaName}`
              : 'toda la base de datos'
        }`,
      );

      const result = await this.executeMaintenance({
        operation: schedule.operation,
        schemaName: schedule.schemaName,
        tableName: schedule.tableName,
      });
      const executedAt = new Date(result.finishedAt);
      const refreshedSchedule = await this.getOrCreateScheduleRow();
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
        `Mantenimiento automatico completado: ${this.formatOperationLabel(schedule.operation)}`,
      );
    } catch (error) {
      const message =
        error instanceof Error ? error.message : 'Error desconocido';
      this.logger.error(
        `No se pudo ejecutar el programador de mantenimiento: ${message}`,
      );

      try {
        const schedule = await this.getOrCreateScheduleRow();
        await this.updateScheduleExecutionDates(
          schedule.id,
          this.parseOptionalDate(schedule.lastRunAt),
          this.computeNextRunAt(
            new Date(),
            this.toSafeNumber(schedule.intervalDays),
            schedule.runAtTime,
            this.parseOptionalDate(schedule.lastRunAt),
          ),
        );
      } catch {
        // Si tampoco se puede recalcular, evitamos romper el loop.
      }
    } finally {
      this.schedulerInProgress = false;
    }
  }

  private async ensurePersistedNextRunAt(
    schedule: MaintenanceScheduleRow,
    now: Date,
  ) {
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
      UPDATE "management"."database_maintenance_schedule"
      SET
        "lastRunAt" = ${lastRunAt},
        "nextRunAt" = ${nextRunAt},
        "updatedAt" = ${new Date()}
      WHERE "id" = ${id}
    `;
  }

  private async getOrCreateScheduleRow(): Promise<MaintenanceScheduleRow> {
    if (!(await this.tableExists('database_maintenance_schedule'))) {
      return this.createDefaultScheduleRow();
    }

    const existingRows = await this.selectMaintenanceScheduleRows();

    const existing = this.getScheduleRow(this.toUnknownArray(existingRows));
    if (existing) {
      return existing;
    }

    const now = new Date();
    const createdRows = await this.upsertMaintenanceScheduleRow(
      {
        id: this.scheduleRowId,
        isEnabled: this.scheduleDefaults.enabled,
        intervalDays: this.scheduleDefaults.everyDays,
        runAtTime: this.scheduleDefaults.runAtTime,
        operation: this.scheduleDefaults.operation,
        schemaName: null,
        tableName: this.scheduleDefaults.tableName,
        lastRunAt: null,
        nextRunAt: null,
        createdAt: now,
        updatedAt: now,
      },
      false,
    );

    return (
      this.getScheduleRow(this.toUnknownArray(createdRows)) ??
      this.getOrCreateScheduleRow()
    );
  }

  private mapScheduleSummary(
    row: MaintenanceScheduleRow,
  ): MaintenanceScheduleSummary {
    return {
      enabled: row.isEnabled,
      everyDays: this.toSafeNumber(row.intervalDays),
      runAtTime: row.runAtTime,
      operation: row.operation,
      schemaName: row.schemaName,
      tableName: row.tableName,
      lastRunAt: this.toIsoString(row.lastRunAt),
      nextRunAt: this.toIsoString(row.nextRunAt),
      createdAt: this.toIsoString(row.createdAt) ?? new Date().toISOString(),
      updatedAt: this.toIsoString(row.updatedAt) ?? new Date().toISOString(),
    };
  }

  private async normalizeSchedulePayload(payload: Record<string, unknown>) {
    const tableName = await this.normalizeOptionalTableName(payload.tableName);
    const schemaName = this.normalizeRequestedSchemaName(
      payload.schemaName,
      tableName,
    );

    return {
      enabled: this.normalizeBoolean(
        payload.enabled,
        'activar el mantenimiento automatico',
      ),
      everyDays: this.normalizePositiveInteger(
        payload.everyDays,
        'la frecuencia en dias',
        365,
      ),
      runAtTime: this.normalizeTimeValue(payload.runAtTime),
      operation: this.normalizeOperation(payload.operation),
      schemaName,
      tableName,
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

    const candidate = this.buildDateAtTime(now, runAtTime);
    if (candidate.getTime() <= now.getTime()) {
      candidate.setDate(candidate.getDate() + everyDays);
    }

    return candidate;
  }

  private buildDateAtTime(baseDate: Date, runAtTime: string) {
    const [hours, minutes] = runAtTime.split(':').map((value) => Number(value));
    const nextRunAt = new Date(baseDate);
    nextRunAt.setHours(hours ?? 0, minutes ?? 0, 0, 0);
    return nextRunAt;
  }

  private createDefaultScheduleRow(): MaintenanceScheduleRow {
    const now = new Date();
    return {
      id: this.scheduleRowId,
      isEnabled: this.scheduleDefaults.enabled,
      intervalDays: this.scheduleDefaults.everyDays,
      runAtTime: this.scheduleDefaults.runAtTime,
      operation: this.scheduleDefaults.operation,
      schemaName: null,
      tableName: this.scheduleDefaults.tableName,
      lastRunAt: null,
      nextRunAt: null,
      createdAt: now,
      updatedAt: now,
    };
  }

  private async runPsqlCommand(
    operation: MaintenanceOperation,
    tableName: string,
  ): Promise<PsqlExecutionResult> {
    const config = this.parseDatabaseUrl();
    const psqlCommand = this.resolvePsqlCommand();
    const target = this.parseQualifiedTableName(tableName);
    const sql = `${this.buildOperationSql(operation)} "${target.schema}"."${target.table}";`;
    const args = [
      '--host',
      config.host,
      '--port',
      config.port,
      '--username',
      config.username,
      '--dbname',
      config.database,
      '--command',
      sql,
      '--echo-all',
      '--set',
      'ON_ERROR_STOP=1',
    ];

    return new Promise<PsqlExecutionResult>((resolve, reject) => {
      const outputChunks: Buffer[] = [];
      const errorChunks: Buffer[] = [];

      const processRef = spawn(psqlCommand, args, {
        env: {
          ...process.env,
          PGPASSWORD: config.password,
          ...(config.sslMode ? { PGSSLMODE: config.sslMode } : {}),
          ...(config.channelBinding
            ? { PGCHANNELBINDING: config.channelBinding }
            : {}),
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      });

      processRef.stdout.on('data', (chunk: Buffer | string) => {
        outputChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      });

      processRef.stderr.on('data', (chunk: Buffer | string) => {
        errorChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      });

      processRef.on('error', (error: NodeJS.ErrnoException) => {
        if (error.code === 'ENOENT') {
          reject(
            new InternalServerErrorException(
              'No se encontro psql. Configura PSQL_PATH o agrega PostgreSQL al PATH del servidor.',
            ),
          );
          return;
        }

        reject(
          new InternalServerErrorException(
            `No se pudo iniciar psql: ${error.message}`,
          ),
        );
      });

      processRef.on('close', (code) => {
        const outputText = this.normalizeProcessLog(
          `${Buffer.concat(outputChunks).toString('utf8')}\n${Buffer.concat(errorChunks).toString('utf8')}`,
        );

        if (code === 0) {
          resolve({ verboseLog: outputText });
          return;
        }

        reject(
          new InternalServerErrorException(
            outputText || `psql finalizo con codigo ${code ?? 'desconocido'}`,
          ),
        );
      });
    });
  }

  private async listMaintenanceTableNames(schemaName?: string | null) {
    const rows = await this.prisma.$queryRaw<Array<{ tableName: string }>>`
      SELECT (tables.table_schema || '.' || tables.table_name) AS "tableName"
      FROM information_schema.tables AS tables
      WHERE tables.table_schema IN ('accounts', 'catalog', 'management')
        AND (${schemaName ?? null}::text IS NULL OR tables.table_schema = ${schemaName ?? null})
        AND tables.table_type = 'BASE TABLE'
        AND NOT (
          tables.table_schema = 'management'
          AND tables.table_name = '_prisma_migrations'
        )
      ORDER BY tables.table_schema ASC, tables.table_name ASC
    `;

    return rows.map((item) => item.tableName);
  }

  private normalizeOptionalSchemaName(rawValue: unknown) {
    const normalized =
      this.normalizeOptionalText(rawValue)?.toLowerCase() ?? '';

    if (!normalized) {
      return null;
    }

    if (
      !this.maintenanceSchemas.includes(
        normalized as (typeof this.maintenanceSchemas)[number],
      )
    ) {
      throw new BadRequestException('Nombre de esquema invalido');
    }

    return normalized;
  }

  private normalizeRequestedSchemaName(
    rawSchemaName: unknown,
    tableName: string | null,
  ) {
    if (tableName) {
      return this.parseQualifiedTableName(tableName).schema;
    }

    return this.normalizeOptionalSchemaName(rawSchemaName);
  }

  private async normalizeOptionalTableName(rawValue: unknown) {
    const normalized =
      this.normalizeOptionalText(rawValue)?.toLowerCase() ?? '';

    if (!normalized) {
      return null;
    }

    if (!/^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)?$/.test(normalized)) {
      throw new BadRequestException('Nombre de tabla invalido');
    }

    const tables = await this.listMaintenanceTableNames();
    if (!tables.includes(normalized)) {
      throw new NotFoundException('La tabla seleccionada no existe');
    }

    return normalized;
  }

  private async selectMaintenanceScheduleRows() {
    const hasSchemaNameColumn = await this.hasMaintenanceScheduleSchemaColumn();
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
          "operation",
          ${schemaSelection},
          "tableName",
          "lastRunAt",
          "nextRunAt",
          "createdAt",
          "updatedAt"
        FROM "management"."database_maintenance_schedule"
        WHERE "id" = $1
        LIMIT 1
      `,
      this.scheduleRowId,
    );
  }

  private async upsertMaintenanceScheduleRow(
    row: {
      id: number;
      isEnabled: boolean;
      intervalDays: number;
      runAtTime: string;
      operation: MaintenanceOperation;
      schemaName: string | null;
      tableName: string | null;
      lastRunAt: Date | null;
      nextRunAt: Date | null;
      createdAt: Date;
      updatedAt: Date;
    },
    overwriteExisting: boolean,
  ) {
    const hasSchemaNameColumn = await this.hasMaintenanceScheduleSchemaColumn();
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
          "operation" = EXCLUDED."operation",
          ${schemaUpdate}
          "tableName" = EXCLUDED."tableName",
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
          row.operation,
          row.schemaName,
          row.tableName,
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
          row.operation,
          row.tableName,
          row.lastRunAt,
          row.nextRunAt,
          row.createdAt,
          row.updatedAt,
        ];

    const sql = hasSchemaNameColumn
      ? `
        INSERT INTO "management"."database_maintenance_schedule" (
          "id",
          "isEnabled",
          "intervalDays",
          "runAtTime",
          "operation"${schemaColumns},
          "tableName",
          "lastRunAt",
          "nextRunAt",
          "createdAt",
          "updatedAt"
        )
        VALUES ($1, $2, $3, $4, $5${schemaValuePlaceholder}, $7, $8, $9, $10, $11)
        ON CONFLICT ("id")
        ${conflictAction}
        RETURNING
          "id",
          "isEnabled",
          "intervalDays",
          "runAtTime",
          "operation",
          ${schemaSelection},
          "tableName",
          "lastRunAt",
          "nextRunAt",
          "createdAt",
          "updatedAt"
      `
      : `
        INSERT INTO "management"."database_maintenance_schedule" (
          "id",
          "isEnabled",
          "intervalDays",
          "runAtTime",
          "operation",
          "tableName",
          "lastRunAt",
          "nextRunAt",
          "createdAt",
          "updatedAt"
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT ("id")
        ${conflictAction}
        RETURNING
          "id",
          "isEnabled",
          "intervalDays",
          "runAtTime",
          "operation",
          ${schemaSelection},
          "tableName",
          "lastRunAt",
          "nextRunAt",
          "createdAt",
          "updatedAt"
      `;

    return this.prisma.$queryRawUnsafe(sql, ...params);
  }

  private async hasMaintenanceScheduleSchemaColumn() {
    if (this.maintenanceScheduleSchemaColumnExists !== null) {
      return this.maintenanceScheduleSchemaColumnExists;
    }

    const rows = await this.prisma.$queryRaw<Array<{ exists: boolean }>>`
      SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'management'
          AND table_name = 'database_maintenance_schedule'
          AND column_name = 'schemaName'
      ) AS "exists"
    `;

    this.maintenanceScheduleSchemaColumnExists = rows[0]?.exists === true;
    return this.maintenanceScheduleSchemaColumnExists;
  }

  private normalizeOperation(rawValue: unknown): MaintenanceOperation {
    const normalized = (this.normalizeOptionalText(rawValue) ?? '')
      .toUpperCase()
      .replace(/\s+/g, '_');

    if (normalized.includes('FULL')) {
      throw new BadRequestException('VACUUM FULL no esta permitido');
    }

    if (
      normalized === 'VACUUM' ||
      normalized === 'ANALYZE' ||
      normalized === 'VACUUM_ANALYZE'
    ) {
      return normalized;
    }

    throw new BadRequestException('Operacion de mantenimiento invalida');
  }

  private buildOperationSql(operation: MaintenanceOperation) {
    if (operation === 'VACUUM_ANALYZE') {
      return 'VACUUM ANALYZE';
    }

    return operation;
  }

  private formatOperationLabel(operation: MaintenanceOperation) {
    if (operation === 'VACUUM_ANALYZE') {
      return 'VACUUM ANALYZE';
    }

    return operation;
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

  private async ensureRequiredTable(tableName: string, message: string) {
    if (await this.tableExists(tableName)) {
      return;
    }

    throw new InternalServerErrorException(message);
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

    return {
      host: parsed.hostname || 'localhost',
      port: parsed.port || '5432',
      username: decodeURIComponent(parsed.username),
      password: decodeURIComponent(parsed.password),
      database,
      schema,
      sslMode: this.normalizeSslMode(parsed.searchParams.get('sslmode')),
      channelBinding: this.normalizeChannelBinding(
        parsed.searchParams.get('channel_binding'),
      ),
    };
  }

  private resolvePsqlCommand() {
    const configured = process.env.PSQL_PATH?.trim();
    if (configured) {
      return configured;
    }

    return process.platform === 'win32' ? 'psql.exe' : 'psql';
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
      !this.maintenanceSchemas.includes(
        schema as (typeof this.maintenanceSchemas)[number],
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

  private normalizeOptionalText(value: unknown) {
    return typeof value === 'string' ? value.trim() : null;
  }

  private toUnknownArray(value: unknown): unknown[] {
    return Array.isArray(value) ? value : [];
  }

  private getScheduleRow(rows: unknown[]): MaintenanceScheduleRow | null {
    if (rows.length === 0) {
      return null;
    }

    const candidate = rows[0];
    if (!candidate || typeof candidate !== 'object') {
      return null;
    }

    const row = candidate as Partial<MaintenanceScheduleRow>;
    if (
      typeof row.id !== 'number' ||
      typeof row.isEnabled !== 'boolean' ||
      typeof row.runAtTime !== 'string' ||
      typeof row.operation !== 'string'
    ) {
      return null;
    }

    return row as MaintenanceScheduleRow;
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

    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  private formatLogDate(value: Date) {
    return value.toLocaleString('es-MX', {
      hour12: true,
    });
  }

  private formatLogStamp(value: Date) {
    return value.toISOString();
  }
}
