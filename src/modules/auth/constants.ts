import 'dotenv/config';
import type { SignOptions } from 'jsonwebtoken';

export const jwtConstants = {
  secret: process.env.JWT_SECRET?.trim() || 'SUPER_SECRET_KEY',
  expiresIn: (process.env.JWT_EXPIRES_IN?.trim() || '1d') as SignOptions['expiresIn'],
} as const;
