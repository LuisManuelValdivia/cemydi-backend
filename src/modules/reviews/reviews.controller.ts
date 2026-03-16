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
import { CreateReviewDto } from './dto/create-review.dto';
import { ReviewsService } from './reviews.service';

@Controller('reviews')
@UsePipes(new ValidationPipe({ whitelist: true, transform: true }))
export class ReviewsController {
  constructor(private readonly reviewsService: ReviewsService) {}

  @Get('product/:productId')
  listApprovedByProduct(@Param('productId', ParseIntPipe) productId: number) {
    return this.reviewsService.listApprovedByProduct(productId);
  }

  @Get('product/:productId/mine')
  getMyReviewByProduct(
    @Headers('authorization') authorization: string | undefined,
    @Param('productId', ParseIntPipe) productId: number,
  ) {
    return this.reviewsService.getMyByProduct(authorization, productId);
  }

  @Post()
  submit(
    @Headers('authorization') authorization: string | undefined,
    @Body() dto: CreateReviewDto,
  ) {
    return this.reviewsService.submit(authorization, dto);
  }

  @Get('admin')
  listForAdmin(
    @Headers('authorization') authorization: string | undefined,
    @Query('status') status: string | undefined,
    @Query('userId') userIdRaw: string | undefined,
  ) {
    const userId = userIdRaw ? Number(userIdRaw) : undefined;
    return this.reviewsService.listForAdmin(authorization, {
      status,
      userId: Number.isInteger(userId) ? userId : undefined,
    });
  }

  @Patch(':id/approve')
  approve(
    @Headers('authorization') authorization: string | undefined,
    @Param('id', ParseIntPipe) id: number,
  ) {
    return this.reviewsService.approve(authorization, id);
  }

  @Delete(':id')
  remove(
    @Headers('authorization') authorization: string | undefined,
    @Param('id', ParseIntPipe) id: number,
  ) {
    return this.reviewsService.remove(authorization, id);
  }
}
