import { Rol } from '@prisma/client';

export type AuthUser = {
  sub: number;
  correo: string;
  rol: Rol;
  sid?: string;
  iat?: number;
  exp?: number;
};
