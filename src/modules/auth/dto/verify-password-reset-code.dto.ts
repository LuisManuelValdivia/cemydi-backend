import { IsEmail, IsNotEmpty, IsString } from 'class-validator';

export class VerifyPasswordResetCodeDto {
  @IsEmail()
  @IsNotEmpty()
  correo: string;

  @IsString()
  @IsNotEmpty()
  codigo: string;
}
