import { IsEmail, IsNotEmpty } from 'class-validator';

export class EmailActionDto {
  @IsEmail()
  @IsNotEmpty()
  correo: string;
}
