import {
  BadRequestException,
  Injectable,
  NotFoundException,
  UnauthorizedException,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { Prisma, ReviewStatus } from '@prisma/client';
import { PrismaService } from '../../prisma/prisma.service';
import {
  extractBearerToken,
  requireAdminRole,
  verifySessionToken,
} from '../auth/session.util';
import { CreateReviewDto } from './dto/create-review.dto';

@Injectable()
export class ReviewsService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly jwtService: JwtService,
  ) {}

  async listApprovedByProduct(productId: number) {
    const reviews = await this.prisma.review.findMany({
      where: {
        productId,
        status: ReviewStatus.APPROVED,
      },
      include: {
        user: {
          select: {
            id: true,
            nombre: true,
          },
        },
      },
      orderBy: { createdAt: 'desc' },
    });

    const count = reviews.length;
    const sum = reviews.reduce((acc, item) => acc + item.rating, 0);
    const averageRating = count > 0 ? Number((sum / count).toFixed(2)) : 0;

    return {
      reviews: reviews.map((item) => ({
        id: item.id,
        rating: item.rating,
        comment: item.comment,
        createdAt: item.createdAt,
        user: item.user,
      })),
      summary: {
        count,
        averageRating,
      },
    };
  }

  async submit(authorization: string | undefined, dto: CreateReviewDto) {
    const payload = this.getSessionPayload(authorization);

    const user = await this.prisma.user.findUnique({
      where: { id: payload.sub },
      select: { id: true, activo: true },
    });

    if (!user || !user.activo) {
      throw new UnauthorizedException('No autenticado');
    }

    const product = await this.prisma.product.findUnique({
      where: { id: dto.productId },
      select: { id: true, activo: true },
    });

    if (!product || !product.activo) {
      throw new NotFoundException('Producto no encontrado');
    }

    const existingReview = await this.prisma.review.findUnique({
      where: {
        userId_productId: {
          userId: payload.sub,
          productId: dto.productId,
        },
      },
      select: {
        rating: true,
      },
    });

    // Si la reseña ya existe, solo permitimos editar el comentario.
    const ratingToPersist = existingReview?.rating ?? dto.rating;
    const status =
      ratingToPersist >= 4 ? ReviewStatus.APPROVED : ReviewStatus.REJECTED;
    const approvedAt = status === ReviewStatus.APPROVED ? new Date() : null;

    const review = await this.prisma.review.upsert({
      where: {
        userId_productId: {
          userId: payload.sub,
          productId: dto.productId,
        },
      },
      create: {
        productId: dto.productId,
        userId: payload.sub,
        rating: ratingToPersist,
        comment: dto.comment.trim(),
        status,
        approvedAt,
        approvedById: null,
      },
      update: {
        rating: ratingToPersist,
        comment: dto.comment.trim(),
        status,
        approvedAt,
        approvedById: null,
      },
    });

    return {
      message:
        status === ReviewStatus.APPROVED
          ? 'Reseña aprobada automáticamente y publicada'
          : 'Reseña rechazada automáticamente por calificación menor a 4 estrellas',
      review: this.toClientReview(review),
    };
  }

  async getMyByProduct(authorization: string | undefined, productId: number) {
    const payload = this.getSessionPayload(authorization);

    const review = await this.prisma.review.findUnique({
      where: {
        userId_productId: {
          userId: payload.sub,
          productId,
        },
      },
    });

    return {
      review: review ? this.toClientReview(review) : null,
    };
  }

  async listForAdmin(
    authorization: string | undefined,
    params: { status?: string; userId?: number },
  ) {
    this.ensureAdmin(authorization);

    const where: Prisma.ReviewWhereInput = {};

    if (params.status && params.status !== 'ALL') {
      if (
        params.status !== ReviewStatus.PENDING &&
        params.status !== ReviewStatus.APPROVED &&
        params.status !== ReviewStatus.REJECTED
      ) {
        throw new BadRequestException('Estado de reseña inválido');
      }

      where.status = params.status as ReviewStatus;
    }

    if (params.userId !== undefined) {
      where.userId = params.userId;
    }

    const reviews = await this.prisma.review.findMany({
      where,
      include: {
        product: {
          select: {
            id: true,
            nombre: true,
          },
        },
        user: {
          select: {
            id: true,
            nombre: true,
            correo: true,
          },
        },
        approvedBy: {
          select: {
            id: true,
            nombre: true,
            correo: true,
          },
        },
      },
      orderBy: [{ status: 'asc' }, { createdAt: 'desc' }],
    });

    return { reviews };
  }

  async approve(authorization: string | undefined, id: number) {
    const payload = this.ensureAdmin(authorization);

    try {
      const review = await this.prisma.review.update({
        where: { id },
        data: {
          status: ReviewStatus.APPROVED,
          approvedAt: new Date(),
          approvedById: payload.sub,
        },
        include: {
          product: {
            select: {
              id: true,
              nombre: true,
            },
          },
          user: {
            select: {
              id: true,
              nombre: true,
              correo: true,
            },
          },
          approvedBy: {
            select: {
              id: true,
              nombre: true,
              correo: true,
            },
          },
        },
      });

      return {
        message: 'Reseña aprobada y publicada',
        review,
      };
    } catch (error) {
      if (
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.code === 'P2025'
      ) {
        throw new NotFoundException('Reseña no encontrada');
      }

      throw error;
    }
  }

  async remove(authorization: string | undefined, id: number) {
    this.ensureAdmin(authorization);

    try {
      await this.prisma.review.delete({
        where: { id },
      });

      return { message: 'Reseña eliminada correctamente' };
    } catch (error) {
      if (
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.code === 'P2025'
      ) {
        throw new NotFoundException('Reseña no encontrada');
      }

      throw error;
    }
  }

  private getSessionPayload(authorization: string | undefined) {
    const token = extractBearerToken(authorization);
    return verifySessionToken(this.jwtService, token);
  }

  private ensureAdmin(authorization: string | undefined) {
    const payload = this.getSessionPayload(authorization);
    requireAdminRole(payload);
    return payload;
  }

  private toClientReview(review: {
    id: number;
    rating: number;
    comment: string;
    status: ReviewStatus;
    createdAt: Date;
    updatedAt: Date;
  }) {
    return {
      id: review.id,
      rating: review.rating,
      comment: review.comment,
      status: review.status,
      createdAt: review.createdAt,
      updatedAt: review.updatedAt,
    };
  }
}
