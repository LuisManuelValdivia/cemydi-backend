import { Injectable } from '@nestjs/common';
import { PassportStrategy } from '@nestjs/passport';
import type { Request } from 'express';
import { Strategy } from 'passport-jwt';
import type { AuthUser } from './auth-user.interface';
import { AUTH_ACCESS_COOKIE, jwtConstants } from './constants';

function extractJwtFromCookie(req: Request) {
  const cookies = req.cookies as Record<string, unknown> | undefined;
  const raw = cookies?.[AUTH_ACCESS_COOKIE];
  return typeof raw === 'string' && raw.length > 0 ? raw : null;
}

function extractJwtFromAuthorizationHeader(req: Request) {
  const authorizationHeader = req.headers.authorization;
  if (typeof authorizationHeader !== 'string') {
    return null;
  }

  const [scheme, token] = authorizationHeader.trim().split(/\s+/, 2);
  if (!scheme || !token || scheme.toLowerCase() !== 'bearer') {
    return null;
  }

  return token.trim() || null;
}

@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  constructor() {
    // `passport-jwt` exposes loose typings here; we keep the config object explicit.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call
    super({
      jwtFromRequest: (req: Request) =>
        extractJwtFromCookie(req) ?? extractJwtFromAuthorizationHeader(req),
      ignoreExpiration: false,
      secretOrKey: jwtConstants.secret,
    });
  }

  validate(payload: AuthUser) {
    return payload;
  }
}
