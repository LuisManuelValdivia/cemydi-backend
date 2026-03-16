import { TipoAdquisicion } from '@prisma/client';
import {
  IsBoolean,
  IsEnum,
  IsInt,
  IsNumber,
  IsOptional,
  IsString,
  MaxLength,
  Min,
  MinLength,
} from 'class-validator';

export class CreateProductDto {
  @IsString()
  @MinLength(2)
  @MaxLength(120)
  nombre!: string;

  @IsString()
  @MinLength(2)
  @MaxLength(80)
  marca!: string;

  @IsString()
  @MinLength(1)
  @MaxLength(80)
  modelo!: string;

  @IsString()
  @MinLength(5)
  @MaxLength(400)
  descripcion!: string;

  @IsNumber()
  @Min(0)
  precio!: number;

  @IsString()
  @MinLength(2)
  @MaxLength(80)
  clasificacion!: string;

  @IsInt()
  @Min(0)
  stock!: number;

  @IsString()
  @MinLength(2)
  @MaxLength(120)
  proveedor!: string;

  @IsEnum(TipoAdquisicion)
  tipoAdquisicion!: TipoAdquisicion;

  @IsOptional()
  @IsBoolean()
  requiereReceta?: boolean;

  @IsOptional()
  @IsBoolean()
  activo?: boolean;
}
