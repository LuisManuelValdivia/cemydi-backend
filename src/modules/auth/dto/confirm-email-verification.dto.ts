import { IsNotEmpty, IsString } from 'class-validator';

export class ConfirmEmailVerificationDto {
  @IsString()
  @IsNotEmpty()
  token: string;
}
