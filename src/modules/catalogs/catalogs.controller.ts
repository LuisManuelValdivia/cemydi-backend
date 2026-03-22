import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  ParseIntPipe,
  Patch,
  Post,
  UseGuards,
  UsePipes,
  ValidationPipe,
} from '@nestjs/common';
import { Rol } from '@prisma/client';
import { Roles } from '../auth/decorators/roles.decorator';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { RolesGuard } from '../auth/guards/roles.guard';
import { CreateCatalogItemDto } from './dto/create-catalog-item.dto';
import { CatalogsService } from './catalogs.service';

@Controller('catalogs')
@Roles(Rol.ADMIN)
@UseGuards(JwtAuthGuard, RolesGuard)
@UsePipes(new ValidationPipe({ whitelist: true, transform: true }))
export class CatalogsController {
  constructor(private readonly catalogsService: CatalogsService) {}

  @Get()
  findAll() {
    return this.catalogsService.findAll();
  }

  @Post('brands')
  createBrand(@Body() dto: CreateCatalogItemDto) {
    return this.catalogsService.createBrand(dto);
  }

  @Patch('brands/:id')
  updateBrand(
    @Param('id', ParseIntPipe) id: number,
    @Body() dto: CreateCatalogItemDto,
  ) {
    return this.catalogsService.updateBrand(id, dto);
  }

  @Delete('brands/:id')
  deleteBrand(@Param('id', ParseIntPipe) id: number) {
    return this.catalogsService.deleteBrand(id);
  }

  @Post('classifications')
  createClassification(@Body() dto: CreateCatalogItemDto) {
    return this.catalogsService.createClassification(dto);
  }

  @Patch('classifications/:id')
  updateClassification(
    @Param('id', ParseIntPipe) id: number,
    @Body() dto: CreateCatalogItemDto,
  ) {
    return this.catalogsService.updateClassification(id, dto);
  }

  @Delete('classifications/:id')
  deleteClassification(@Param('id', ParseIntPipe) id: number) {
    return this.catalogsService.deleteClassification(id);
  }
}
