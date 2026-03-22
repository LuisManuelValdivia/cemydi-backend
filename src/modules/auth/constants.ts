import 'dotenv/config';
import type { CookieOptions } from 'express';
import type { SignOptions } from 'jsonwebtoken';

const rawSecret = process.env.JWT_SECRET?.trim();

if (!rawSecret) {
  throw new Error(
    'JWT_SECRET es obligatorio. Defínelo en las variables de entorno (process.env.JWT_SECRET).',
  );
}

export const AUTH_ACCESS_COOKIE = 'cemydi_access';

function authCookieBase(): Pick<
  CookieOptions,
  'httpOnly' | 'secure' | 'sameSite' | 'path'
> {
  return {
    httpOnly: true,
    secure: false,
    sameSite: 'lax',
    path: '/',
  };
}

export function buildAuthCookieSetOptions(maxAgeMs: number): CookieOptions {
  return { ...authCookieBase(), maxAge: maxAgeMs };
}

export function buildAuthCookieClearOptions(): CookieOptions {
  return authCookieBase();
}

export const jwtConstants = {
  secret: rawSecret,
  expiresIn: (process.env.JWT_EXPIRES_IN?.trim() ||
    '1d') as SignOptions['expiresIn'],
} as const;
