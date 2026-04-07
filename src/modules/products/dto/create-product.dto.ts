import { TipoAdquisicion } from '@prisma/client';
import { Transform, Type } from 'class-transformer';
import {
  IsArray,
  IsBoolean,
  IsEnum,
  IsInt,
  IsNumber,
  IsOptional,
  IsString,
  IsUrl,
  MaxLength,
  Min,
  MinLength,
} from 'class-validator';
import {
  transformBoolean,
  transformNumber,
  transformStringArray,
} from './product-dto.helpers';

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

  @Type(() => Number)
  @Transform(transformNumber)
  @IsNumber()
  @Min(0)
  precio!: number;

  @IsString()
  @MinLength(2)
  @MaxLength(80)
  clasificacion!: string;

  @Type(() => Number)
  @Transform(transformNumber)
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
  @Transform(transformBoolean)
  @IsBoolean()
  requiereReceta?: boolean;

  @IsOptional()
  @Transform(transformBoolean)
  @IsBoolean()
  activo?: boolean;

  @IsOptional()
  @Transform(transformStringArray)
  @IsArray()
  @IsUrl(
    {
      require_protocol: true,
    },
    { each: true },
  )
  imageUrls?: string[];
}
