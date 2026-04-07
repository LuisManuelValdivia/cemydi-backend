import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Prisma, PrismaClient } from '@prisma/client';
import type { AuthUser } from '../modules/auth/auth-user.interface';

type PrismaConnectionRole = 'admin' | 'client' | 'reader';

@Injectable()
export class PrismaService
  extends PrismaClient
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(PrismaService.name);
  private readonly maxConnectAttempts = 5;
  private readonly connectRetryDelayMs = 2_000;
  private readonly maxQueryRetryAttempts = 2;
  private readonly clientDb = this.createScopedClient(
    process.env.DATABASE_URL_CLIENT,
  );
  private readonly readerDb = this.createScopedClient(
    process.env.DATABASE_URL_READER,
  );
  private readonly retryableReadActions = new Set<string>([
    'findUnique',
    'findUniqueOrThrow',
    'findFirst',
    'findFirstOrThrow',
    'findMany',
    'count',
    'aggregate',
    'groupBy',
    'queryRaw',
    'findRaw',
    'aggregateRaw',
  ]);
  private readonly reconnectPromises = new Map<
    PrismaConnectionRole,
    Promise<void> | null
  >();

  constructor() {
    super();

    this.installRetryMiddleware(this, 'admin');

    if (this.clientDb) {
      this.installRetryMiddleware(this.clientDb, 'client');
    }

    if (this.readerDb) {
      this.installRetryMiddleware(this.readerDb, 'reader');
    }
  }

  async onModuleInit() {
    for (const [role, client] of this.getRegisteredClients()) {
      await this.connectWithRetry(client, role);
    }
  }

  async onModuleDestroy() {
    for (const [, client] of this.getRegisteredClients()) {
      await client.$disconnect();
    }
  }

  asAdmin() {
    return this as PrismaClient;
  }

  asClient() {
    return this.clientDb ?? this.asAdmin();
  }

  asReader() {
    return this.readerDb ?? this.clientDb ?? this.asAdmin();
  }

  forUser(user?: Pick<AuthUser, 'rol'> | null) {
    if (!user) {
      return this.asReader();
    }

    return user.rol === 'ADMIN' ? this.asAdmin() : this.asClient();
  }

  private createScopedClient(url: string | undefined) {
    const normalizedUrl = url?.trim();
    if (!normalizedUrl) {
      return null;
    }

    return new PrismaClient({
      datasources: {
        db: {
          url: normalizedUrl,
        },
      },
    });
  }

  private getRegisteredClients() {
    const clients = new Map<
      PrismaClient,
      [PrismaConnectionRole, PrismaClient]
    >();
    clients.set(this, ['admin', this]);

    if (this.clientDb) {
      clients.set(this.clientDb, ['client', this.clientDb]);
    }

    if (this.readerDb) {
      clients.set(this.readerDb, ['reader', this.readerDb]);
    }

    return Array.from(clients.values());
  }

  private installRetryMiddleware(
    client: PrismaClient,
    role: PrismaConnectionRole,
  ) {
    client.$use(async (params, next) => {
      let attempt = 1;

      while (true) {
        try {
          const result: unknown = await next(params);
          return result;
        } catch (error) {
          if (!this.shouldRetryQuery(params, error, attempt)) {
            throw error;
          }

          this.logger.warn(
            `Fallo transitorio en ${this.describeQuery(role, params)}. Reintentando ${attempt}/${this.maxQueryRetryAttempts - 1} tras reconectar Prisma...`,
          );
          await this.reconnect(client, role);
          attempt += 1;
        }
      }
    });
  }

  private async reconnect(client: PrismaClient, role: PrismaConnectionRole) {
    const reconnectPromise = this.reconnectPromises.get(role);
    if (reconnectPromise) {
      await reconnectPromise;
      return;
    }

    const nextReconnectPromise = (async () => {
      try {
        await client.$disconnect().catch(() => undefined);
        await this.connectWithRetry(client, role);
      } finally {
        this.reconnectPromises.set(role, null);
      }
    })();

    this.reconnectPromises.set(role, nextReconnectPromise);
    await nextReconnectPromise;
  }

  private async connectWithRetry(
    client: PrismaClient,
    role: PrismaConnectionRole,
  ) {
    let lastError: unknown = null;

    for (let attempt = 1; attempt <= this.maxConnectAttempts; attempt += 1) {
      try {
        await client.$connect();
        if (attempt > 1) {
          this.logger.log(
            `Conexion Prisma (${role}) restablecida en el intento ${attempt}/${this.maxConnectAttempts}`,
          );
        }
        return;
      } catch (error) {
        lastError = error;
        const retryable = this.isRetryableConnectionError(error);

        if (!retryable || attempt === this.maxConnectAttempts) {
          throw error;
        }

        this.logger.warn(
          `No se pudo conectar a la base de datos (${role}) en el intento ${attempt}/${this.maxConnectAttempts}. Reintentando en ${this.connectRetryDelayMs / 1000}s...`,
        );
        await this.delay(this.connectRetryDelayMs);
      }
    }

    throw lastError;
  }

  private isRetryableConnectionError(error: unknown) {
    if (!error || typeof error !== 'object') {
      return false;
    }

    const candidate = error as { code?: string; message?: string };
    const message = candidate.message?.toLowerCase() ?? '';

    return (
      candidate.code === 'P1017' ||
      candidate.code === 'P2024' ||
      candidate.code === 'P1001' ||
      message.includes("can't reach database server") ||
      message.includes('server has closed the connection') ||
      message.includes(
        'timed out fetching a new connection from the connection pool',
      ) ||
      message.includes('connection pool') ||
      message.includes('database server') ||
      message.includes('timed out')
    );
  }

  private shouldRetryQuery(
    params: Prisma.MiddlewareParams,
    error: unknown,
    attempt: number,
  ) {
    return (
      attempt < this.maxQueryRetryAttempts &&
      this.retryableReadActions.has(params.action) &&
      this.isRetryableConnectionError(error)
    );
  }

  private describeQuery(
    role: PrismaConnectionRole,
    params: Prisma.MiddlewareParams,
  ) {
    return `${role}.${params.model ?? 'raw'}.${params.action}`;
  }

  private delay(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
