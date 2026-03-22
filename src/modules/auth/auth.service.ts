import {
  BadRequestException,
  Injectable,
  Logger,
  UnauthorizedException,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { AuthTokenPurpose, Prisma, Rol } from '@prisma/client';
import * as bcrypt from 'bcrypt';
import { createHash, randomBytes, randomInt, randomUUID } from 'crypto';
import { MailService } from '../mail/mail.service';
import { PrismaService } from '../../prisma/prisma.service';
import type { AuthUser } from './auth-user.interface';
import { buildEmailVerificationConfirmUrl, authFlowConstants } from './constants';
import { ConfirmPasswordResetDto } from './dto/confirm-password-reset.dto';
import { EmailActionDto } from './dto/email-action.dto';
import { LoginDto } from './dto/login.dto';
import { RegisterDto } from './dto/register.dto';
import { RequestPasswordResetDto } from './dto/request-password-reset.dto';
import { VerifyPasswordResetCodeDto } from './dto/verify-password-reset-code.dto';

type PublicUser = {
  id: number;
  nombre: string;
  correo: string;
  activo: boolean;
  rol: Rol;
  emailVerifiedAt: Date | null;
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
    private mailService: MailService,
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
        nombre: dto.nombre.trim(),
        correo,
        password: hashedPassword,
      },
    });

    await this.issueEmailVerificationLink(user.id, user.correo, user.nombre);

    return {
      message: 'Usuario registrado correctamente. Revisa tu correo para verificar la cuenta.',
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

    if (!user.emailVerifiedAt) {
      await this.registerLoginAttempt({
        userId: user.id,
        correo: user.correo,
        success: false,
        reason: 'EMAIL_NOT_VERIFIED',
      });
      throw new UnauthorizedException(
          'Debes verificar tu correo antes de iniciar sesión',
      );
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
      accessToken,
      user: this.serializeUser(user),
      cookieMaxAgeMs: Math.max(0, expiresAt.getTime() - now.getTime()),
    };
  }

  async resendEmailVerification(dto: EmailActionDto) {
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
      return {
        message: 'Si el correo existe, enviaremos un enlace de verificación.',
      };
    }

    if (user.emailVerifiedAt) {
      return {
        message: 'La cuenta ya fue verificada.',
      };
    }

    await this.issueEmailVerificationLink(user.id, user.correo, user.nombre);

    return {
      message: 'Si el correo existe, enviaremos un enlace de verificación.',
    };
  }

  async confirmEmailVerification(token: string) {
    const normalizedToken = token.trim();
    if (!normalizedToken) {
      throw new BadRequestException('Token inválido');
    }

    const tokenHash = this.hashValue(normalizedToken);
    const now = new Date();
    const authToken = await this.prisma.authToken.findFirst({
      where: {
        purpose: AuthTokenPurpose.EMAIL_VERIFICATION_LINK,
        tokenHash,
      },
      include: {
        user: true,
      },
    });

    if (!authToken || authToken.consumedAt || authToken.expiresAt <= now) {
      throw new BadRequestException('El enlace de verificación es inválido o expiró');
    }

    await this.prisma.$transaction([
      this.prisma.user.update({
        where: { id: authToken.userId },
        data: {
          emailVerifiedAt: now,
        },
      }),
      this.prisma.authToken.update({
        where: { id: authToken.id },
        data: {
          consumedAt: now,
        },
      }),
      this.prisma.authToken.updateMany({
        where: {
          userId: authToken.userId,
          purpose: AuthTokenPurpose.EMAIL_VERIFICATION_LINK,
          consumedAt: null,
          id: {
            not: authToken.id,
          },
        },
        data: {
          consumedAt: now,
        },
      }),
    ]);

    return {
      message: 'Correo verificado correctamente',
    };
  }

  async requestPasswordReset(dto: RequestPasswordResetDto) {
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
      return {
        message: 'Si el correo existe, enviaremos un código para restablecer la contraseña.',
      };
    }

    const code = this.generateNumericCode();
    const now = new Date();
    const expiresAt = new Date(
      now.getTime() + authFlowConstants.passwordResetExpiresMinutes * 60 * 1000,
    );

    await this.prisma.$transaction([
      this.prisma.authToken.updateMany({
        where: {
          userId: user.id,
          purpose: AuthTokenPurpose.PASSWORD_RESET_CODE,
          consumedAt: null,
        },
        data: {
          consumedAt: now,
        },
      }),
      this.prisma.authToken.create({
        data: {
          userId: user.id,
          correo: user.correo,
          purpose: AuthTokenPurpose.PASSWORD_RESET_CODE,
          codeHash: this.hashValue(code),
          expiresAt,
        },
      }),
    ]);

    await this.mailService.sendPasswordResetCode({
      correo: user.correo,
      nombre: user.nombre,
      code,
    });

    return {
      message: 'Si el correo existe, enviaremos un código para restablecer la contraseña.',
    };
  }

  async verifyPasswordResetCode(dto: VerifyPasswordResetCodeDto) {
    const authToken = await this.getValidPasswordResetToken(
      dto.correo,
      dto.codigo,
      true,
    );

    return {
      message: 'Código verificado correctamente',
      expiresAt: authToken.expiresAt.toISOString(),
    };
  }

  async confirmPasswordReset(dto: ConfirmPasswordResetDto) {
    const now = new Date();
    const authToken = await this.getValidPasswordResetToken(
      dto.correo,
      dto.codigo,
      true,
    );

    const passwordHash = await bcrypt.hash(dto.newPassword, 10);

    await this.prisma.$transaction([
      this.prisma.user.update({
        where: { id: authToken.userId },
        data: {
          password: passwordHash,
        },
      }),
      this.prisma.authToken.update({
        where: { id: authToken.id },
        data: {
          consumedAt: now,
        },
      }),
      this.prisma.userSession.updateMany({
        where: {
          userId: authToken.userId,
          endedAt: null,
        },
        data: {
          endedAt: now,
          lastSeenAt: now,
        },
      }),
    ]);

    return {
      message: 'Contraseña actualizada correctamente',
    };
  }

  async tryLogoutWithToken(token: string | null | undefined): Promise<void> {
    if (!token?.trim()) {
      return;
    }
    try {
      const payload = this.jwtService.verify<AuthUser>(token);
      await this.logout(payload);
    } catch {
      /* token inválido o expirado */
    }
  }

  async logout(user: AuthUser) {
    if (user.sid) {
      await this.prisma.userSession.updateMany({
        where: {
          tokenId: user.sid,
          endedAt: null,
        },
        data: {
          endedAt: new Date(),
          lastSeenAt: new Date(),
        },
      });
    }

    return {
      message: 'Sesión cerrada correctamente',
    };
  }

  async getSecurityOverview(
    user: AuthUser,
  ): Promise<{ overview: SecurityOverviewResponse }> {
    const now = new Date();

    if (user.sid) {
      await this.prisma.userSession.updateMany({
        where: {
          tokenId: user.sid,
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
      emailVerified: Boolean(user.emailVerifiedAt),
      emailVerifiedAt: user.emailVerifiedAt?.toISOString() ?? null,
    };
  }

  private async issueEmailVerificationLink(userId: number, correo: string, nombre: string) {
    const rawToken = randomBytes(32).toString('hex');
    const now = new Date();
    const expiresAt = new Date(
      now.getTime() + authFlowConstants.emailVerificationExpiresMinutes * 60 * 1000,
    );

    await this.prisma.$transaction([
      this.prisma.authToken.updateMany({
        where: {
          userId,
          purpose: AuthTokenPurpose.EMAIL_VERIFICATION_LINK,
          consumedAt: null,
        },
        data: {
          consumedAt: now,
        },
      }),
      this.prisma.authToken.create({
        data: {
          userId,
          correo,
          purpose: AuthTokenPurpose.EMAIL_VERIFICATION_LINK,
          tokenHash: this.hashValue(rawToken),
          expiresAt,
        },
      }),
    ]);

    const verificationUrl = buildEmailVerificationConfirmUrl(rawToken);

    await this.mailService.sendEmailVerificationLink({
      correo,
      nombre,
      verificationUrl,
    });
  }

  private generateNumericCode() {
    return `${randomInt(0, 1000000)}`.padStart(6, '0');
  }

  private async getValidPasswordResetToken(
    correoInput: string,
    codigoInput: string,
    registerAttemptOnFailure: boolean,
  ) {
    const correo = correoInput.trim().toLowerCase();
    const codigo = codigoInput.trim();
    const now = new Date();
    const authToken = await this.prisma.authToken.findFirst({
      where: {
        correo,
        purpose: AuthTokenPurpose.PASSWORD_RESET_CODE,
        consumedAt: null,
      },
      include: {
        user: true,
      },
      orderBy: [{ createdAt: 'desc' }],
    });

    if (!authToken || authToken.expiresAt <= now) {
      throw new BadRequestException('El código es inválido o expiró');
    }

    if (authToken.attemptCount >= authFlowConstants.passwordResetMaxAttempts) {
      throw new BadRequestException('El código excedió el número de intentos permitidos');
    }

    const codeHash = this.hashValue(codigo);
    if (!authToken.codeHash || authToken.codeHash !== codeHash) {
      if (registerAttemptOnFailure) {
        await this.prisma.authToken.update({
          where: { id: authToken.id },
          data: {
            attemptCount: {
              increment: 1,
            },
          },
        });
      }
      throw new BadRequestException('El código es inválido o expiró');
    }

    return authToken;
  }

  private hashValue(value: string) {
    return createHash('sha256').update(value).digest('hex');
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
        'No se pudo registrar el intento de login porque faltan tablas de auditoría. Ejecuta la migración auth_security_tracking.',
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
        'El login continuó sin guardar sesión/auditoría porque faltan tablas de monitoreo. Ejecuta la migración auth_security_tracking.',
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
