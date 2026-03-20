import { Body, Controller, Get, Headers, Post } from '@nestjs/common';
import { AuthService } from './auth.service';
import { RegisterDto } from './dto/register.dto';
import { LoginDto } from './dto/login.dto';

@Controller('auth')
export class AuthController {
  constructor(private readonly authService: AuthService) {}

  @Post('register')
  register(@Body() dto: RegisterDto) {
    return this.authService.register(dto);
  }

  @Post('login')
  login(@Body() dto: LoginDto) {
    return this.authService.login(dto);
  }

  @Post('logout')
  logout(@Headers('authorization') authorization: string | undefined) {
    return this.authService.logout(authorization);
  }

  @Get('security-overview')
  getSecurityOverview(
    @Headers('authorization') authorization: string | undefined,
  ) {
    return this.authService.getSecurityOverview(authorization);
  }
}
