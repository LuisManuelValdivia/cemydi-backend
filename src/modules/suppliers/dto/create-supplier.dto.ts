import { IsString, MaxLength, MinLength } from 'class-validator';

export class CreateSupplierDto {
  @IsString()
  @MinLength(2)
  @MaxLength(120)
  nombre!: string;

  @IsString()
  @MinLength(2)
  @MaxLength(120)
  encargado!: string;

  @IsString()
  @MinLength(2)
  @MaxLength(120)
  repartidor!: string;

  @IsString()
  @MinLength(5)
  @MaxLength(180)
  direccion!: string;
}
