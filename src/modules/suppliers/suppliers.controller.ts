import {
  Body,
  Controller,
  Get,
  Headers,
  Post,
  UsePipes,
  ValidationPipe,
} from '@nestjs/common';
import { CreateSupplierDto } from './dto/create-supplier.dto';
import { SuppliersService } from './suppliers.service';

@Controller('suppliers')
@UsePipes(new ValidationPipe({ whitelist: true, transform: true }))
export class SuppliersController {
  constructor(private readonly suppliersService: SuppliersService) {}

  @Get()
  findAll(@Headers('authorization') authorization: string | undefined) {
    return this.suppliersService.findAll(authorization);
  }

  @Post()
  create(
    @Headers('authorization') authorization: string | undefined,
    @Body() dto: CreateSupplierDto,
  ) {
    return this.suppliersService.create(authorization, dto);
  }
}
