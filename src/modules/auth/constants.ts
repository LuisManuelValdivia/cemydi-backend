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

function parseBooleanEnv(value: string | undefined) {
  const normalized = value?.trim().toLowerCase();
  if (!normalized) {
    return null;
  }

  if (['true', '1', 'yes', 'si'].includes(normalized)) {
    return true;
  }

  if (['false', '0', 'no'].includes(normalized)) {
    return false;
  }

  return null;
}

function shouldUseSecureCookies() {
  const explicitSecure = parseBooleanEnv(process.env.AUTH_COOKIE_SECURE);
  if (explicitSecure !== null) {
    return explicitSecure;
  }

  const configuredOrigins = (process.env.CORS_ORIGIN ?? '')
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean);

  if (configuredOrigins.length === 0) {
    return false;
  }

  return configuredOrigins.some((origin) => {
    try {
      const parsed = new URL(origin);
      return parsed.protocol === 'https:' && parsed.hostname !== 'localhost';
    } catch {
      return false;
    }
  });
}

function authCookieBase(): Pick<
  CookieOptions,
  'httpOnly' | 'secure' | 'sameSite' | 'path'
> {
  const secureCookies = shouldUseSecureCookies();

  return {
    httpOnly: true,
    secure: secureCookies,
    sameSite: secureCookies ? 'none' : 'lax',
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
