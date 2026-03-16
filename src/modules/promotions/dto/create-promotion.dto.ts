import {
  IsDateString,
  IsEnum,
  IsInt,
  IsOptional,
  IsString,
  MaxLength,
  Min,
  MinLength,
  ValidateIf,
} from 'class-validator';

export enum PromotionMode {
  PRODUCT = 'PRODUCT',
  CATEGORY = 'CATEGORY',
}

export class CreatePromotionDto {
  @IsEnum(PromotionMode)
  mode!: PromotionMode;

  @ValidateIf((dto: CreatePromotionDto) => dto.mode === PromotionMode.PRODUCT)
  @IsInt()
  @Min(1)
  productId?: number;

  @ValidateIf((dto: CreatePromotionDto) => dto.mode === PromotionMode.CATEGORY)
  @IsString()
  @MinLength(2)
  @MaxLength(80)
  clasificacion?: string;

  @IsDateString()
  startAt!: string;

  @IsDateString()
  endAt!: string;

  @IsString()
  @MinLength(5)
  @MaxLength(240)
  descripcion!: string;

  @IsOptional()
  @IsString()
  @MaxLength(500)
  imageUrl?: string;
}
