import {
  Body,
  Controller,
  Get,
  Post,
  Query,
  Req,
  Res,
  UseGuards,
} from '@nestjs/common';
import { Rol } from '@prisma/client';
import type { Request, Response } from 'express';
import { ExtractJwt } from 'passport-jwt';
import { AuthService } from './auth.service';
import type { AuthUser } from './auth-user.interface';
import {
  AUTH_ACCESS_COOKIE,
  buildFrontendLoginUrl,
  buildAuthCookieClearOptions,
  buildAuthCookieSetOptions,
} from './constants';
import { CurrentUser } from './decorators/current-user.decorator';
import { Roles } from './decorators/roles.decorator';
import { RegisterDto } from './dto/register.dto';
import { ConfirmEmailVerificationDto } from './dto/confirm-email-verification.dto';
import { ConfirmPasswordResetDto } from './dto/confirm-password-reset.dto';
import { EmailActionDto } from './dto/email-action.dto';
import { LoginDto } from './dto/login.dto';
import { RequestPasswordResetDto } from './dto/request-password-reset.dto';
import { VerifyPasswordResetCodeDto } from './dto/verify-password-reset-code.dto';
import { JwtAuthGuard } from './guards/jwt-auth.guard';
import { RolesGuard } from './guards/roles.guard';

@Controller('auth')
export class AuthController {
  constructor(private readonly authService: AuthService) {}

  private extractAccessToken(req: Request): string | null {
    const fromCookie = req.cookies?.[AUTH_ACCESS_COOKIE];
    if (typeof fromCookie === 'string' && fromCookie.trim()) {
      return fromCookie.trim();
    }
    return ExtractJwt.fromAuthHeaderAsBearerToken()(req) ?? null;
  }

  @Post('register')
  register(@Body() dto: RegisterDto) {
    return this.authService.register(dto);
  }

  @Post('login')
  async login(
    @Body() dto: LoginDto,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    const result = await this.authService.login(dto);
    res.cookie(
      AUTH_ACCESS_COOKIE,
      result.accessToken,
      buildAuthCookieSetOptions(result.cookieMaxAgeMs, req),
    );
    return { user: result.user };
  }

  @Post('email-verification/send')
  resendEmailVerification(@Body() dto: EmailActionDto) {
    return this.authService.resendEmailVerification(dto);
  }

  @Get('email-verification/confirm')
  async confirmEmailVerification(
    @Query() dto: ConfirmEmailVerificationDto,
    @Res() res: Response,
  ) {
    try {
      await this.authService.confirmEmailVerification(dto.token);
      return res.redirect(buildFrontendLoginUrl({ verified: 'success' }));
    } catch {
      return res.redirect(buildFrontendLoginUrl({ verified: 'error' }));
    }
  }

  @Post('password-reset/request')
  requestPasswordReset(@Body() dto: RequestPasswordResetDto) {
    return this.authService.requestPasswordReset(dto);
  }

  @Post('password-reset/verify-code')
  verifyPasswordResetCode(@Body() dto: VerifyPasswordResetCodeDto) {
    return this.authService.verifyPasswordResetCode(dto);
  }

  @Post('password-reset/confirm')
  confirmPasswordReset(@Body() dto: ConfirmPasswordResetDto) {
    return this.authService.confirmPasswordReset(dto);
  }

  @Post('logout')
  async logout(@Req() req: Request, @Res({ passthrough: true }) res: Response) {
    const token = this.extractAccessToken(req);
    res.clearCookie(AUTH_ACCESS_COOKIE, buildAuthCookieClearOptions(req));
    await this.authService.tryLogoutWithToken(token);
    return {
      message: 'Sesion cerrada correctamente',
    };
  }

  @Get('security-overview')
  @Roles(Rol.ADMIN)
  @UseGuards(JwtAuthGuard, RolesGuard)
  getSecurityOverview(@CurrentUser() user: AuthUser) {
    return this.authService.getSecurityOverview(user);
  }
}
