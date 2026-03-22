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
  UseGuards,
  UsePipes,
  ValidationPipe,
} from '@nestjs/common';
import { Rol } from '@prisma/client';
import type { AuthUser } from '../auth/auth-user.interface';
import { CurrentUser } from '../auth/decorators/current-user.decorator';
import { Roles } from '../auth/decorators/roles.decorator';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { RolesGuard } from '../auth/guards/roles.guard';
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
  @UseGuards(JwtAuthGuard)
  getMyReviewByProduct(
    @CurrentUser() user: AuthUser,
    @Param('productId', ParseIntPipe) productId: number,
  ) {
    return this.reviewsService.getMyByProduct(user, productId);
  }

  @Post()
  @UseGuards(JwtAuthGuard)
  submit(@CurrentUser() user: AuthUser, @Body() dto: CreateReviewDto) {
    return this.reviewsService.submit(user, dto);
  }

  @Get('admin')
  @Roles(Rol.ADMIN)
  @UseGuards(JwtAuthGuard, RolesGuard)
  listForAdmin(
    @Query('status') status: string | undefined,
    @Query('userId') userIdRaw: string | undefined,
  ) {
    const userId = userIdRaw ? Number(userIdRaw) : undefined;
    return this.reviewsService.listForAdmin({
      status,
      userId: Number.isInteger(userId) ? userId : undefined,
    });
  }

  @Patch(':id/approve')
  @Roles(Rol.ADMIN)
  @UseGuards(JwtAuthGuard, RolesGuard)
  approve(@CurrentUser() user: AuthUser, @Param('id', ParseIntPipe) id: number) {
    return this.reviewsService.approve(user, id);
  }

  @Delete(':id')
  @Roles(Rol.ADMIN)
  @UseGuards(JwtAuthGuard, RolesGuard)
  remove(@Param('id', ParseIntPipe) id: number) {
    return this.reviewsService.remove(id);
  }
}
