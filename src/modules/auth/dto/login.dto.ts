import { IsEmail, IsNotEmpty } from 'class-validator';

export class LoginDto {
  @IsEmail()
  correo: string;

  @IsNotEmpty()
  password: string;
}
