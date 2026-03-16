import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../../prisma/prisma.service';
import {
  extractBearerToken,
  requireAdminRole,
  verifySessionToken,
} from '../auth/session.util';
import { CreatePromotionDto, PromotionMode } from './dto/create-promotion.dto';
import { UpdatePromotionDto } from './dto/update-promotion.dto';

type FindPromotionsParams = {
  includeExpired: boolean;
  authorization: string | undefined;
};

@Injectable()
export class PromotionsService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly jwtService: JwtService,
  ) {}

  async findAll(params: FindPromotionsParams) {
    const where: Prisma.PromotionWhereInput = {};

    if (params.includeExpired) {
      this.ensureAdmin(params.authorization);
    } else {
      const now = new Date();
      where.startAt = { lte: now };
      where.endAt = { gte: now };
      where.product = {
        activo: true,
        stock: { gt: 0 },
      };
    }

    const promotions = await this.prisma.promotion.findMany({
      where,
      include: {
        product: {
          select: {
            id: true,
            nombre: true,
            clasificacion: true,
            precio: true,
            stock: true,
            activo: true,
          },
        },
      },
      orderBy: [{ startAt: 'asc' }, { createdAt: 'desc' }],
    });

    return { promotions };
  }

  async create(authorization: string | undefined, dto: CreatePromotionDto) {
    this.ensureAdmin(authorization);

    const startAt = new Date(dto.startAt);
    const endAt = new Date(dto.endAt);

    if (Number.isNaN(startAt.getTime()) || Number.isNaN(endAt.getTime())) {
      throw new BadRequestException('Fechas de promocion invalidas');
    }

    if (startAt >= endAt) {
      throw new BadRequestException(
        'La fecha final debe ser posterior a la fecha de inicio',
      );
    }

    const imageUrl = dto.imageUrl?.trim() || null;
    const descripcion = dto.descripcion.trim();

    if (dto.mode === PromotionMode.PRODUCT) {
      if (!dto.productId) {
        throw new BadRequestException('Selecciona un producto');
      }

      const product = await this.prisma.product.findUnique({
        where: { id: dto.productId },
        select: { id: true },
      });

      if (!product) {
        throw new BadRequestException('Producto no encontrado');
      }

      const promotion = await this.prisma.promotion.create({
        data: {
          productId: dto.productId,
          descripcion,
          startAt,
          endAt,
          imageUrl,
        },
        include: {
          product: {
            select: {
              id: true,
              nombre: true,
              clasificacion: true,
              precio: true,
              stock: true,
              activo: true,
            },
          },
        },
      });

      return {
        message: 'Promocion creada correctamente',
        promotions: [promotion],
      };
    }

    const clasificacion = dto.clasificacion?.trim();
    if (!clasificacion) {
      throw new BadRequestException('Selecciona una clasificacion');
    }

    const products = await this.prisma.product.findMany({
      where: {
        clasificacion: clasificacion,
        activo: true,
      },
      select: { id: true },
    });

    if (products.length === 0) {
      throw new BadRequestException(
        'No hay productos activos en esa clasificacion',
      );
    }

    const promotions = await this.prisma.$transaction(
      products.map((product) =>
        this.prisma.promotion.create({
          data: {
            productId: product.id,
            descripcion,
            startAt,
            endAt,
            imageUrl,
          },
          include: {
            product: {
              select: {
                id: true,
                nombre: true,
                clasificacion: true,
                precio: true,
                stock: true,
                activo: true,
              },
            },
          },
        }),
      ),
    );

    return {
      message: `Se crearon ${promotions.length} promociones`,
      promotions,
    };
  }

  async update(
    authorization: string | undefined,
    id: number,
    dto: UpdatePromotionDto,
  ) {
    this.ensureAdmin(authorization);

    const currentPromotion = await this.prisma.promotion.findUnique({
      where: { id },
      select: {
        id: true,
        startAt: true,
        endAt: true,
      },
    });

    if (!currentPromotion) {
      throw new NotFoundException('Promocion no encontrada');
    }

    const nextStartAt = dto.startAt
      ? new Date(dto.startAt)
      : currentPromotion.startAt;
    const nextEndAt = dto.endAt ? new Date(dto.endAt) : currentPromotion.endAt;

    if (
      Number.isNaN(nextStartAt.getTime()) ||
      Number.isNaN(nextEndAt.getTime())
    ) {
      throw new BadRequestException('Fechas de promocion invalidas');
    }

    if (nextStartAt >= nextEndAt) {
      throw new BadRequestException(
        'La fecha final debe ser posterior a la fecha de inicio',
      );
    }

    const data: Prisma.PromotionUpdateInput = {};

    if (dto.productId !== undefined) {
      const product = await this.prisma.product.findUnique({
        where: { id: dto.productId },
        select: { id: true },
      });

      if (!product) {
        throw new BadRequestException('Producto no encontrado');
      }

      data.product = {
        connect: {
          id: dto.productId,
        },
      };
    }

    if (dto.startAt !== undefined) {
      data.startAt = nextStartAt;
    }

    if (dto.endAt !== undefined) {
      data.endAt = nextEndAt;
    }

    if (dto.descripcion !== undefined) {
      data.descripcion = dto.descripcion.trim();
    }

    if (dto.imageUrl !== undefined) {
      data.imageUrl = dto.imageUrl.trim() || null;
    }

    if (Object.keys(data).length === 0) {
      throw new BadRequestException('No hay cambios para actualizar');
    }

    try {
      const promotion = await this.prisma.promotion.update({
        where: { id },
        data,
        include: {
          product: {
            select: {
              id: true,
              nombre: true,
              clasificacion: true,
              precio: true,
              stock: true,
              activo: true,
            },
          },
        },
      });

      return {
        message: 'Promocion actualizada correctamente',
        promotion,
      };
    } catch (error) {
      if (
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.code === 'P2025'
      ) {
        throw new NotFoundException('Promocion no encontrada');
      }

      throw error;
    }
  }

  async remove(authorization: string | undefined, id: number) {
    this.ensureAdmin(authorization);

    try {
      await this.prisma.promotion.delete({ where: { id } });
      return { message: 'Promocion eliminada correctamente' };
    } catch (error) {
      if (
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.code === 'P2025'
      ) {
        throw new NotFoundException('Promocion no encontrada');
      }

      throw error;
    }
  }

  private ensureAdmin(authorization: string | undefined) {
    const token = extractBearerToken(authorization);
    const payload = verifySessionToken(this.jwtService, token);
    requireAdminRole(payload);
  }
}
