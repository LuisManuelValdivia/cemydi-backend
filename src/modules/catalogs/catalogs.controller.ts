import {
  Body,
  Controller,
  Get,
  Headers,
  Post,
  UsePipes,
  ValidationPipe,
} from '@nestjs/common';
import { CreateCatalogItemDto } from './dto/create-catalog-item.dto';
import { CatalogsService } from './catalogs.service';

@Controller('catalogs')
@UsePipes(new ValidationPipe({ whitelist: true, transform: true }))
export class CatalogsController {
  constructor(private readonly catalogsService: CatalogsService) {}

  @Get()
  findAll(@Headers('authorization') authorization: string | undefined) {
    return this.catalogsService.findAll(authorization);
  }

  @Post('brands')
  createBrand(
    @Headers('authorization') authorization: string | undefined,
    @Body() dto: CreateCatalogItemDto,
  ) {
    return this.catalogsService.createBrand(authorization, dto);
  }

  @Post('classifications')
  createClassification(
    @Headers('authorization') authorization: string | undefined,
    @Body() dto: CreateCatalogItemDto,
  ) {
    return this.catalogsService.createClassification(authorization, dto);
  }
}
