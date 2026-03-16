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
import { CreatePromotionDto } from './dto/create-promotion.dto';
import { UpdatePromotionDto } from './dto/update-promotion.dto';
import { PromotionsService } from './promotions.service';

@Controller('promotions')
@UsePipes(new ValidationPipe({ whitelist: true, transform: true }))
export class PromotionsController {
  constructor(private readonly promotionsService: PromotionsService) {}

  @Get()
  findAll(
    @Query('includeExpired') includeExpiredRaw: string | undefined,
    @Headers('authorization') authorization: string | undefined,
  ) {
    return this.promotionsService.findAll({
      includeExpired: includeExpiredRaw === 'true',
      authorization,
    });
  }

  @Post()
  create(
    @Headers('authorization') authorization: string | undefined,
    @Body() dto: CreatePromotionDto,
  ) {
    return this.promotionsService.create(authorization, dto);
  }

  @Patch(':id')
  update(
    @Headers('authorization') authorization: string | undefined,
    @Param('id', ParseIntPipe) id: number,
    @Body() dto: UpdatePromotionDto,
  ) {
    return this.promotionsService.update(authorization, id, dto);
  }

  @Delete(':id')
  remove(
    @Headers('authorization') authorization: string | undefined,
    @Param('id', ParseIntPipe) id: number,
  ) {
    return this.promotionsService.remove(authorization, id);
  }
}
