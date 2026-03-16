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

export class UpdateProductDto {
  @IsOptional()
  @IsString()
  @MinLength(2)
  @MaxLength(120)
  nombre?: string;

  @IsOptional()
  @IsString()
  @MinLength(2)
  @MaxLength(80)
  marca?: string;

  @IsOptional()
  @IsString()
  @MinLength(1)
  @MaxLength(80)
  modelo?: string;

  @IsOptional()
  @IsString()
  @MinLength(5)
  @MaxLength(400)
  descripcion?: string;

  @IsOptional()
  @IsNumber()
  @Min(0)
  precio?: number;

  @IsOptional()
  @IsString()
  @MinLength(2)
  @MaxLength(80)
  clasificacion?: string;

  @IsOptional()
  @IsInt()
  @Min(0)
  stock?: number;

  @IsOptional()
  @IsString()
  @MinLength(2)
  @MaxLength(120)
  proveedor?: string;

  @IsOptional()
  @IsEnum(TipoAdquisicion)
  tipoAdquisicion?: TipoAdquisicion;

  @IsOptional()
  @IsBoolean()
  requiereReceta?: boolean;

  @IsOptional()
  @IsBoolean()
  activo?: boolean;
}
