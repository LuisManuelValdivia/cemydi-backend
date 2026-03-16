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
  Query,
  UsePipes,
  ValidationPipe,
} from '@nestjs/common';
import { CreateProductDto } from './dto/create-product.dto';
import { UpdateProductDto } from './dto/update-product.dto';
import { ProductsService } from './products.service';

@Controller('products')
@UsePipes(new ValidationPipe({ whitelist: true, transform: true }))
export class ProductsController {
  constructor(private readonly productsService: ProductsService) {}

  @Get()
  findAll(
    @Query('search') search: string | undefined,
    @Query('clasificaciones') clasificacionesRaw: string | undefined,
    @Query('tipos') tiposRaw: string | undefined,
    @Query('requiereReceta') requiereRecetaRaw: string | undefined,
    @Query('page') pageRaw: string | undefined,
    @Query('pageSize') pageSizeRaw: string | undefined,
    @Query('includeInactive') includeInactiveRaw: string | undefined,
    @Headers('authorization') authorization: string | undefined,
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
      authorization,
    );
  }

  @Get(':id')
  findOne(
    @Param('id', ParseIntPipe) id: number,
    @Query('includeInactive') includeInactiveRaw: string | undefined,
    @Headers('authorization') authorization: string | undefined,
  ) {
    return this.productsService.findOne(
      id,
      includeInactiveRaw === 'true',
      authorization,
    );
  }

  @Post()
  create(
    @Headers('authorization') authorization: string | undefined,
    @Body() dto: CreateProductDto,
  ) {
    return this.productsService.create(authorization, dto);
  }

  @Patch(':id')
  update(
    @Headers('authorization') authorization: string | undefined,
    @Param('id', ParseIntPipe) id: number,
    @Body() dto: UpdateProductDto,
  ) {
    return this.productsService.update(authorization, id, dto);
  }

  @Delete(':id')
  remove(
    @Headers('authorization') authorization: string | undefined,
    @Param('id', ParseIntPipe) id: number,
  ) {
    return this.productsService.remove(authorization, id);
  }
}
