import 'dotenv/config';
import type { CookieOptions, Request } from 'express';
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

function isSecureRequest(req?: Request) {
  const forwardedProto = req?.headers['x-forwarded-proto'];
  const forwardedProtoValue = Array.isArray(forwardedProto)
    ? forwardedProto[0]
    : forwardedProto;

  if (typeof forwardedProtoValue === 'string') {
    const normalizedProto = forwardedProtoValue.split(',')[0]?.trim().toLowerCase();
    if (normalizedProto === 'https') {
      return true;
    }
  }

  if (req?.secure) {
    return true;
  }

  const originHeader = req?.headers.origin;
  if (typeof originHeader === 'string') {
    try {
      return new URL(originHeader).protocol === 'https:';
    } catch {
      // Ignorar origen invalido y seguir con la deteccion por configuracion.
    }
  }

  return false;
}

function shouldUseSecureCookies(req?: Request) {
  const explicitSecure = parseBooleanEnv(process.env.AUTH_COOKIE_SECURE);
  if (explicitSecure !== null) {
    return explicitSecure;
  }

  if (isSecureRequest(req)) {
    return true;
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

function authCookieBase(req?: Request): Pick<
  CookieOptions,
  'httpOnly' | 'secure' | 'sameSite' | 'path'
> {
  const secureCookies = shouldUseSecureCookies(req);

  return {
    httpOnly: true,
    secure: secureCookies,
    sameSite: secureCookies ? 'none' : 'lax',
    path: '/',
  };
}

export function buildAuthCookieSetOptions(maxAgeMs: number, req?: Request): CookieOptions {
  return { ...authCookieBase(req), maxAge: maxAgeMs };
}

export function buildAuthCookieClearOptions(req?: Request): CookieOptions {
  return authCookieBase(req);
}

export const jwtConstants = {
  secret: rawSecret,
  expiresIn: (process.env.JWT_EXPIRES_IN?.trim() ||
    '1d') as SignOptions['expiresIn'],
} as const;
