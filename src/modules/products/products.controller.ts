import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  ParseIntPipe,
  Patch,
  Post,
  Query,
  UploadedFiles,
  UseGuards,
  UseInterceptors,
  UsePipes,
  ValidationPipe,
} from '@nestjs/common';
import { FilesInterceptor } from '@nestjs/platform-express';
import { Rol } from '@prisma/client';
import type { AuthUser } from '../auth/auth-user.interface';
import { CurrentUser } from '../auth/decorators/current-user.decorator';
import { Roles } from '../auth/decorators/roles.decorator';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { OptionalJwtAuthGuard } from '../auth/guards/optional-jwt-auth.guard';
import { RolesGuard } from '../auth/guards/roles.guard';
import { CreateProductDto } from './dto/create-product.dto';
import { UpdateProductDto } from './dto/update-product.dto';
import { ProductsService } from './products.service';

type UploadedProductFile = {
  fieldname: string;
  originalname: string;
  encoding: string;
  mimetype: string;
  size: number;
  buffer: Buffer;
};

@Controller('products')
@UsePipes(new ValidationPipe({ whitelist: true, transform: true }))
export class ProductsController {
  constructor(private readonly productsService: ProductsService) {}

  @Get()
  @UseGuards(OptionalJwtAuthGuard)
  findAll(
    @Query('search') search: string | undefined,
    @Query('clasificaciones') clasificacionesRaw: string | undefined,
    @Query('tipos') tiposRaw: string | undefined,
    @Query('requiereReceta') requiereRecetaRaw: string | undefined,
    @Query('page') pageRaw: string | undefined,
    @Query('pageSize') pageSizeRaw: string | undefined,
    @Query('includeInactive') includeInactiveRaw: string | undefined,
    @CurrentUser() user: AuthUser | undefined,
  ) {
    const clasificaciones = clasificacionesRaw
      ? clasificacionesRaw
          .split(',')
          .map((item) => item.trim())
          .filter(Boolean)
      : [];
    const tipos = tiposRaw
      ? tiposRaw
          .split(',')
          .map((item) => item.trim())
          .filter(Boolean)
      : [];

    return this.productsService.findAll(
      {
        search,
        clasificaciones,
        tipos,
        requiereRecetaRaw,
        pageRaw,
        pageSizeRaw,
        includeInactive: includeInactiveRaw === 'true',
      },
      user,
    );
  }

  @Get(':id')
  @UseGuards(OptionalJwtAuthGuard)
  findOne(
    @Param('id', ParseIntPipe) id: number,
    @Query('includeInactive') includeInactiveRaw: string | undefined,
    @CurrentUser() user: AuthUser | undefined,
  ) {
    return this.productsService.findOne(
      id,
      includeInactiveRaw === 'true',
      user,
    );
  }

  @Post()
  @Roles(Rol.ADMIN)
  @UseGuards(JwtAuthGuard, RolesGuard)
  @UseInterceptors(FilesInterceptor('images', 10))
  create(
    @Body() dto: CreateProductDto,
    @UploadedFiles() files: UploadedProductFile[] = [],
  ) {
    return this.productsService.create(dto, files);
  }

  @Patch(':id')
  @Roles(Rol.ADMIN)
  @UseGuards(JwtAuthGuard, RolesGuard)
  @UseInterceptors(FilesInterceptor('images', 10))
  update(
    @Param('id', ParseIntPipe) id: number,
    @Body() dto: UpdateProductDto,
    @UploadedFiles() files: UploadedProductFile[] = [],
  ) {
    return this.productsService.update(id, dto, files);
  }

  @Delete(':id')
  @Roles(Rol.ADMIN)
  @UseGuards(JwtAuthGuard, RolesGuard)
  remove(@Param('id', ParseIntPipe) id: number) {
    return this.productsService.remove(id);
  }
}
