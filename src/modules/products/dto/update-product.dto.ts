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
  transformIntegerArray,
  transformNumber,
  transformStringArray,
} from './product-dto.helpers';

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
  @Type(() => Number)
  @Transform(transformNumber)
  @IsNumber()
  @Min(0)
  precio?: number;

  @IsOptional()
  @IsString()
  @MinLength(2)
  @MaxLength(80)
  clasificacion?: string;

  @IsOptional()
  @Type(() => Number)
  @Transform(transformNumber)
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

  @IsOptional()
  @Transform(transformIntegerArray)
  @IsArray()
  @IsInt({ each: true })
  keepImageIds?: number[];
}
