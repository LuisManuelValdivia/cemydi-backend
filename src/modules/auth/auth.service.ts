import {
  BadRequestException,
  Logger,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { Prisma, Rol } from '@prisma/client';
import * as bcrypt from 'bcrypt';
import { randomUUID } from 'crypto';
import { PrismaService } from '../../prisma/prisma.service';
import { LoginDto } from './dto/login.dto';
import { RegisterDto } from './dto/register.dto';
import {
  extractBearerToken,
  requireAdminRole,
  verifySessionToken,
} from './session.util';

type PublicUser = {
  id: number;
  nombre: string;
  correo: string;
  activo: boolean;
  rol: Rol;
};

type SecurityOverviewResponse = {
  activeSessions: Array<{
    sessionId: string;
    userId: number;
    nombre: string;
    correo: string;
    rol: Rol;
    createdAt: string;
    lastSeenAt: string;
    expiresAt: string;
  }>;
  loginAttempts: Array<{
    id: number;
    userId: number | null;
    nombre: string;
    correo: string;
    success: boolean;
    reason: string | null;
    attemptedAt: string;
  }>;
  summary: {
    activeSessions: number;
    recentAttempts: number;
    failedAttempts: number;
  };
};

type JwtPayloadWithExp = {
  exp?: number;
};

@Injectable()
export class AuthService {
  private readonly logger = new Logger(AuthService.name);

  constructor(
    private prisma: PrismaService,
    private jwtService: JwtService,
  ) {}

  async register(dto: RegisterDto) {
    const correo = dto.correo.trim().toLowerCase();
    const userExists = await this.prisma.user.findFirst({
      where: {
        correo: {
          equals: correo,
          mode: 'insensitive',
        },
      },
    });

    if (userExists) {
      throw new BadRequestException('El usuario ya existe');
    }

    const hashedPassword = await bcrypt.hash(dto.password, 10);

    const user = await this.prisma.user.create({
      data: {
        nombre: dto.nombre,
        correo,
        password: hashedPassword,
      },
    });

    return {
      message: 'Usuario registrado correctamente',
      user: this.serializeUser(user),
    };
  }

  async login(dto: LoginDto) {
    const correo = dto.correo.trim().toLowerCase();
    const user = await this.prisma.user.findFirst({
      where: {
        correo: {
          equals: correo,
          mode: 'insensitive',
        },
      },
    });

    if (!user) {
      await this.registerLoginAttempt({
        correo,
        success: false,
        reason: 'USER_NOT_FOUND',
      });
      throw new UnauthorizedException('Credenciales invalidas');
    }

    const passwordValid = await bcrypt.compare(dto.password, user.password);

    if (!passwordValid) {
      await this.registerLoginAttempt({
        userId: user.id,
        correo: user.correo,
        success: false,
        reason: 'INVALID_PASSWORD',
      });
      throw new UnauthorizedException('Credenciales invalidas');
    }

    if (!user.activo) {
      await this.registerLoginAttempt({
        userId: user.id,
        correo: user.correo,
        success: false,
        reason: 'USER_INACTIVE',
      });
      throw new UnauthorizedException('Tu usuario esta dado de baja');
    }

    const sessionId = randomUUID();
    const payload = {
      sub: user.id,
      correo: user.correo,
      rol: user.rol,
      sid: sessionId,
    };
    const accessToken = this.jwtService.sign(payload);
    const decodedToken: unknown = this.jwtService.decode(accessToken);
    const tokenExpiration = this.hasTokenExpiration(decodedToken)
      ? decodedToken.exp
      : undefined;
    const now = new Date();
    const expiresAt = tokenExpiration
      ? new Date(tokenExpiration * 1000)
      : new Date(now.getTime() + 24 * 60 * 60 * 1000);

    await this.persistAuthTracking({
      userId: user.id,
      correo: user.correo,
      sessionId,
      expiresAt,
      success: true,
      reason: 'LOGIN_OK',
      attemptedAt: now,
    });

    return {
      access_token: accessToken,
      user: this.serializeUser(user),
    };
  }

  async logout(authorization: string | undefined) {
    const token = extractBearerToken(authorization);
    const payload = verifySessionToken(this.jwtService, token);

    if (payload.sid) {
      await this.prisma.userSession.updateMany({
        where: {
          tokenId: payload.sid,
          endedAt: null,
        },
        data: {
          endedAt: new Date(),
          lastSeenAt: new Date(),
        },
      });
    }

    return {
      message: 'Sesion cerrada correctamente',
    };
  }

  async getSecurityOverview(
    authorization: string | undefined,
  ): Promise<{ overview: SecurityOverviewResponse }> {
    const payload = this.ensureAdmin(authorization);
    const now = new Date();

    if (payload.sid) {
      await this.prisma.userSession.updateMany({
        where: {
          tokenId: payload.sid,
          endedAt: null,
        },
        data: {
          lastSeenAt: now,
        },
      });
    }

    const [activeSessions, loginAttempts] = await Promise.all([
      this.prisma.userSession.findMany({
        where: {
          endedAt: null,
          expiresAt: {
            gt: now,
          },
        },
        include: {
          user: {
            select: {
              id: true,
              nombre: true,
              correo: true,
              rol: true,
            },
          },
        },
        orderBy: [{ lastSeenAt: 'desc' }, { createdAt: 'desc' }],
        take: 8,
      }),
      this.prisma.loginAttempt.findMany({
        include: {
          user: {
            select: {
              id: true,
              nombre: true,
              correo: true,
            },
          },
        },
        orderBy: [{ attemptedAt: 'desc' }, { id: 'desc' }],
        take: 12,
      }),
    ]);

    const serializedAttempts = loginAttempts.map((attempt) => ({
      id: attempt.id,
      userId: attempt.userId,
      nombre: attempt.user?.nombre ?? this.resolveAuditName(attempt.correo),
      correo: attempt.user?.correo ?? attempt.correo,
      success: attempt.success,
      reason: attempt.reason,
      attemptedAt: attempt.attemptedAt.toISOString(),
    }));

    return {
      overview: {
        activeSessions: activeSessions.map((session) => ({
          sessionId: session.id,
          userId: session.userId,
          nombre: session.user.nombre,
          correo: session.user.correo,
          rol: session.user.rol,
          createdAt: session.createdAt.toISOString(),
          lastSeenAt: session.lastSeenAt.toISOString(),
          expiresAt: session.expiresAt.toISOString(),
        })),
        loginAttempts: serializedAttempts,
        summary: {
          activeSessions: activeSessions.length,
          recentAttempts: serializedAttempts.length,
          failedAttempts: serializedAttempts.filter(
            (attempt) => !attempt.success,
          ).length,
        },
      },
    };
  }

  private serializeUser(user: PublicUser) {
    return {
      id: user.id,
      nombre: user.nombre,
      correo: user.correo,
      activo: user.activo,
      rol: user.rol,
    };
  }

  private async registerLoginAttempt(input: {
    userId?: number;
    correo: string;
    success: boolean;
    reason: string;
  }) {
    try {
      await this.prisma.loginAttempt.create({
        data: {
          userId: input.userId,
          correo: input.correo,
          success: input.success,
          reason: input.reason,
        },
      });
    } catch (error) {
      if (this.isMissingTrackingTableError(error)) {
        this.logger.warn(
          'No se pudo registrar el intento de login porque faltan tablas de auditoria. Ejecuta la migracion auth_security_tracking.',
        );
        return;
      }

      throw error;
    }
  }

  private async persistAuthTracking(input: {
    userId: number;
    correo: string;
    sessionId: string;
    expiresAt: Date;
    success: boolean;
    reason: string;
    attemptedAt: Date;
  }) {
    try {
      await this.prisma.$transaction([
        this.prisma.userSession.create({
          data: {
            id: input.sessionId,
            tokenId: input.sessionId,
            userId: input.userId,
            createdAt: input.attemptedAt,
            lastSeenAt: input.attemptedAt,
            expiresAt: input.expiresAt,
          },
        }),
        this.prisma.loginAttempt.create({
          data: {
            userId: input.userId,
            correo: input.correo,
            success: input.success,
            reason: input.reason,
            attemptedAt: input.attemptedAt,
          },
        }),
      ]);
    } catch (error) {
      if (this.isMissingTrackingTableError(error)) {
        this.logger.warn(
          'El login continuo sin guardar sesion/auditoria porque faltan tablas de monitoreo. Ejecuta la migracion auth_security_tracking.',
        );
        return;
      }

      throw error;
    }
  }

  private resolveAuditName(correo: string) {
    const [localPart] = correo.split('@');
    if (!localPart) return 'Usuario desconocido';

    return localPart
      .split(/[._-]+/)
      .filter(Boolean)
      .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
      .join(' ');
  }

  private ensureAdmin(authorization: string | undefined) {
    const token = extractBearerToken(authorization);
    const payload = verifySessionToken(this.jwtService, token);
    requireAdminRole(payload);
    return payload;
  }

  private isMissingTrackingTableError(error: unknown) {
    return (
      error instanceof Prisma.PrismaClientKnownRequestError &&
      error.code === 'P2021'
    );
  }

  private hasTokenExpiration(value: unknown): value is JwtPayloadWithExp {
    return (
      typeof value === 'object' &&
      value !== null &&
      (!('exp' in value) ||
        typeof (value as { exp?: unknown }).exp === 'number')
    );
  }
}
