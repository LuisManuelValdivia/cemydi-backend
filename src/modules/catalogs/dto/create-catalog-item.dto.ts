import { IsString, MaxLength, MinLength } from 'class-validator';

export class CreateCatalogItemDto {
  @IsString()
  @MinLength(2)
  @MaxLength(80)
  nombre!: string;
}
