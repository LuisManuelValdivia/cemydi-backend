import { Injectable } from '@nestjs/common';
import { AuthGuard } from '@nestjs/passport';
import type { AuthUser } from '../auth-user.interface';

@Injectable()
export class OptionalJwtAuthGuard extends AuthGuard('jwt') {
  handleRequest<TUser = AuthUser | null>(
    err: unknown,
    user: AuthUser | false | null | undefined,
    _info: unknown,
    _context: unknown,
    _status?: unknown,
  ): TUser {
    if (err) {
      return null as TUser;
    }

    return (user || null) as TUser;
  }
}
