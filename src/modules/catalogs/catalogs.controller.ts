import {
  Body,
  Controller,
  Delete,
  Get,
  Headers,
  Param,
  ParseIntPipe,
  Patch,
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

  @Patch('brands/:id')
  updateBrand(
    @Headers('authorization') authorization: string | undefined,
    @Param('id', ParseIntPipe) id: number,
    @Body() dto: CreateCatalogItemDto,
  ) {
    return this.catalogsService.updateBrand(authorization, id, dto);
  }

  @Delete('brands/:id')
  deleteBrand(
    @Headers('authorization') authorization: string | undefined,
    @Param('id', ParseIntPipe) id: number,
  ) {
    return this.catalogsService.deleteBrand(authorization, id);
  }

  @Post('classifications')
  createClassification(
    @Headers('authorization') authorization: string | undefined,
    @Body() dto: CreateCatalogItemDto,
  ) {
    return this.catalogsService.createClassification(authorization, dto);
  }

  @Patch('classifications/:id')
  updateClassification(
    @Headers('authorization') authorization: string | undefined,
    @Param('id', ParseIntPipe) id: number,
    @Body() dto: CreateCatalogItemDto,
  ) {
    return this.catalogsService.updateClassification(authorization, id, dto);
  }

  @Delete('classifications/:id')
  deleteClassification(
    @Headers('authorization') authorization: string | undefined,
    @Param('id', ParseIntPipe) id: number,
  ) {
    return this.catalogsService.deleteClassification(authorization, id);
  }
}
