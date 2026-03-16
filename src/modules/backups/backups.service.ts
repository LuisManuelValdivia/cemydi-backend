import {
  BadRequestException,
  Injectable,
  InternalServerErrorException,
  NotFoundException,
} from '@nestjs/common';
import { spawn } from 'node:child_process';
import { JwtService } from '@nestjs/jwt';
import { PrismaService } from '../../prisma/prisma.service';
import {
  extractBearerToken,
  requireAdminRole,
  verifySessionToken,
} from '../auth/session.util';

type BackupRecordSummary = {
  id: number;
  fileName: string;
  sizeBytes: number;
  createdAt: string;
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
export class BackupsService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly jwtService: JwtService,
  ) {}

  async createDatabaseBackupRecord(authorization: string | undefined) {
    this.ensureAdmin(authorization);

    const backup = await this.buildDatabaseBackupPayload();
    return this.insertBackupRecord(backup);
  }

  async createSingleTableBackupRecord(
    authorization: string | undefined,
    tableName: string,
  ) {
    this.ensureAdmin(authorization);

    const backup = await this.buildSingleTableBackupPayload(tableName);
    return this.insertBackupRecord(backup);
  }

  async listDatabaseBackupRecords(authorization: string | undefined) {
    this.ensureAdmin(authorization);

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

  async getDatabaseBackupRecord(
    authorization: string | undefined,
    id: number,
  ) {
    this.ensureAdmin(authorization);

    const rows = await this.prisma.$queryRaw<
      Array<{
        id: number;
        fileName: string;
        content: Buffer | Uint8Array | string;
        sizeBytes: number | bigint | string;
        createdAt: Date | string;
      }>
    >`
      SELECT "id", "fileName", "content", "sizeBytes", "createdAt"
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
      content: this.toBuffer(record.content),
    };
  }

  async createDatabaseBackup(authorization: string | undefined) {
    this.ensureAdmin(authorization);

    const backup = await this.buildDatabaseBackupPayload();

    return {
      fileName: backup.fileName,
      content: backup.content,
    };
  }

  async deleteDatabaseBackupRecord(
    authorization: string | undefined,
    id: number,
  ) {
    this.ensureAdmin(authorization);

    const deletedRows = await this.prisma.$queryRaw<
      Array<{
        id: number;
        fileName: string;
        sizeBytes: number | bigint | string;
        createdAt: Date | string;
      }>
    >`
      DELETE FROM "database_backups"
      WHERE "id" = ${id}
      RETURNING "id", "fileName", "sizeBytes", "createdAt"
    `;

    const deleted = deletedRows[0];
    if (!deleted) {
      throw new NotFoundException('Respaldo no encontrado');
    }

    return this.mapBackupRecordSummary(deleted);
  }

  private async buildDatabaseBackupPayload() {
    const now = new Date();
    const fileName = this.createBackupFileName(now);
    const content = await this.runPgDump();

    return {
      fileName,
      content,
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

    return {
      fileName,
      content,
      createdAt: now,
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

    return {
      date: `${year}${month}${day}`,
      time: `${hour}${minute}${second}`,
    };
  }

  private toBuffer(value: Buffer | Uint8Array | string) {
    if (Buffer.isBuffer(value)) {
      return value;
    }

    if (value instanceof Uint8Array) {
      return Buffer.from(value);
    }

    return Buffer.from(value, 'base64');
  }

  async getDatabaseStatus(authorization: string | undefined) {
    this.ensureAdmin(authorization);

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
    const totalTableBytes = tableStats.reduce((acc, item) => acc + item.sizeBytes, 0);

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
      },
    };
  }

  private ensureAdmin(authorization: string | undefined) {
    const token = extractBearerToken(authorization);
    const payload = verifySessionToken(this.jwtService, token);
    requireAdminRole(payload);
  }

  private async insertBackupRecord(backup: {
    fileName: string;
    content: Buffer;
    createdAt: Date;
  }) {
    const createdRows = await this.prisma.$queryRaw<
      Array<{
        id: number;
        fileName: string;
        sizeBytes: number | bigint | string;
        createdAt: Date | string;
      }>
    >`
      INSERT INTO "database_backups" ("fileName", "content", "sizeBytes", "createdAt")
      VALUES (${backup.fileName}, ${backup.content}, ${backup.content.length}, ${backup.createdAt})
      RETURNING "id", "fileName", "sizeBytes", "createdAt"
    `;

    const created = createdRows[0];
    if (!created) {
      throw new Error('No se pudo guardar el registro del respaldo');
    }

    return this.mapBackupRecordSummary(created);
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
    const normalized = String(tableName ?? '').trim().toLowerCase();

    if (!normalized) {
      throw new BadRequestException('Selecciona una tabla para generar el respaldo');
    }

    if (!/^[a-z][a-z0-9_]*$/.test(normalized)) {
      throw new BadRequestException('Nombre de tabla invalido');
    }

    return normalized;
  }

  private parseDatabaseUrl(): ParsedDatabaseUrl & { schema: string } {
    const rawDatabaseUrl = process.env.DATABASE_DIRECT_URL || process.env.DATABASE_URL;
    if (!rawDatabaseUrl) {
      throw new InternalServerErrorException('DATABASE_URL no esta configurada');
    }

    let parsed: URL;
    try {
      parsed = new URL(rawDatabaseUrl);
    } catch {
      throw new InternalServerErrorException('DATABASE_URL tiene un formato invalido');
    }

    if (!['postgresql:', 'postgres:'].includes(parsed.protocol)) {
      throw new InternalServerErrorException('DATABASE_URL debe apuntar a PostgreSQL');
    }

    const database = decodeURIComponent(parsed.pathname.replace(/^\/+/, ''));
    const schema = (parsed.searchParams.get('schema') ?? 'public').trim();

    if (!database) {
      throw new InternalServerErrorException(
        'DATABASE_URL no contiene el nombre de la base de datos',
      );
    }

    if (!this.isSafeIdentifier(schema)) {
      throw new InternalServerErrorException('El schema de DATABASE_URL es invalido');
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
      args.push(
        '--table',
        `${databaseConfig.schema}.${tableName}`,
      );
    }

    return new Promise<Buffer>((resolve, reject) => {
      const stdoutChunks: Buffer[] = [];
      const stderrChunks: Buffer[] = [];

      const dumpProcess = spawn(pgDumpCommand, args, {
        env: {
          ...process.env,
          PGPASSWORD: databaseConfig.password,
          ...(databaseConfig.sslMode ? { PGSSLMODE: databaseConfig.sslMode } : {}),
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

        const stderrMessage = Buffer.concat(stderrChunks).toString('utf8').trim();
        reject(
          new InternalServerErrorException(
            stderrMessage || `pg_dump finalizo con codigo ${code ?? 'desconocido'}`,
          ),
        );
      });
    });
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
    const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    const value = bytes / 1024 ** exponent;
    return `${value.toFixed(exponent === 0 ? 0 : 2)} ${units[exponent]}`;
  }
}
