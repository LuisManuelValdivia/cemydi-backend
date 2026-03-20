import { UnauthorizedException } from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { Rol } from '@prisma/client';
import { jwtConstants } from './constants';

export type SessionPayload = {
  sub: number;
  correo: string;
  rol: Rol;
  sid?: string;
};

export function extractBearerToken(authorization: string | undefined) {
  if (!authorization) {
    throw new UnauthorizedException('No autenticado');
  }

  const [scheme, token] = authorization.split(' ');

  if (scheme !== 'Bearer' || !token) {
    throw new UnauthorizedException('Token invalido');
  }

  return token;
}

export function verifySessionToken(
  jwtService: JwtService,
  token: string,
): SessionPayload {
  try {
    return jwtService.verify<SessionPayload>(token, {
      secret: jwtConstants.secret,
    });
  } catch {
    throw new UnauthorizedException('Token invalido o expirado');
  }
}

export function requireAdminRole(payload: SessionPayload) {
  if (payload.rol !== Rol.ADMIN) {
    throw new UnauthorizedException('No tienes permisos de administrador');
  }
}
