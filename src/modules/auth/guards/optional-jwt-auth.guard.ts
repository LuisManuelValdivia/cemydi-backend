import { Injectable } from '@nestjs/common';
import { AuthGuard } from '@nestjs/passport';
import type { AuthUser } from '../auth-user.interface';

@Injectable()
export class OptionalJwtAuthGuard extends AuthGuard('jwt') {
  handleRequest<TUser = AuthUser | null>(
    err: unknown,
    user: AuthUser | false | null | undefined,
    info: unknown,
    context: unknown,
    status?: unknown,
  ): TUser {
    void info;
    void context;
    void status;

    if (err) {
      return null as TUser;
    }

    return (user || null) as TUser;
  }
}
