import { IsEmail, IsNotEmpty, MinLength } from 'class-validator';

export class RegisterDto {
  @IsNotEmpty()
  @MinLength(3)
  nombre: string;

  @IsEmail()
  @IsNotEmpty()
  correo: string;

  @MinLength(8)
  @IsNotEmpty()
  password: string;
}
