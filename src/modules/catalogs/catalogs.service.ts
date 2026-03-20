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
import { CreateCatalogItemDto } from './dto/create-catalog-item.dto';

@Injectable()
export class CatalogsService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly jwtService: JwtService,
  ) {}

  async findAll(authorization: string | undefined) {
    this.ensureAdmin(authorization);

    const [brands, classifications] = await Promise.all([
      this.prisma.brand.findMany({ orderBy: { nombre: 'asc' } }),
      this.prisma.classification.findMany({ orderBy: { nombre: 'asc' } }),
    ]);

    return { brands, classifications };
  }

  async createBrand(
    authorization: string | undefined,
    dto: CreateCatalogItemDto,
  ) {
    this.ensureAdmin(authorization);

    try {
      const brand = await this.prisma.brand.create({
        data: { nombre: dto.nombre.trim() },
      });
      return {
        message: 'Marca creada correctamente',
        brand,
      };
    } catch (error) {
      this.throwIfDuplicate(error, 'La marca ya existe');
      throw error;
    }
  }

  async updateBrand(
    authorization: string | undefined,
    id: number,
    dto: CreateCatalogItemDto,
  ) {
    this.ensureAdmin(authorization);

    const existing = await this.prisma.brand.findUnique({ where: { id } });
    if (!existing) {
      throw new NotFoundException('La marca no existe');
    }

    try {
      const brand = await this.prisma.brand.update({
        where: { id },
        data: { nombre: dto.nombre.trim() },
      });
      return {
        message: 'Marca actualizada correctamente',
        brand,
      };
    } catch (error) {
      this.throwIfDuplicate(error, 'La marca ya existe');
      throw error;
    }
  }

  async deleteBrand(authorization: string | undefined, id: number) {
    this.ensureAdmin(authorization);

    const existing = await this.prisma.brand.findUnique({ where: { id } });
    if (!existing) {
      throw new NotFoundException('La marca no existe');
    }

    await this.prisma.brand.delete({ where: { id } });

    return {
      message: 'Marca eliminada correctamente',
    };
  }

  async createClassification(
    authorization: string | undefined,
    dto: CreateCatalogItemDto,
  ) {
    this.ensureAdmin(authorization);

    try {
      const classification = await this.prisma.classification.create({
        data: { nombre: dto.nombre.trim() },
      });
      return {
        message: 'Clasificacion creada correctamente',
        classification,
      };
    } catch (error) {
      this.throwIfDuplicate(error, 'La clasificacion ya existe');
      throw error;
    }
  }

  async updateClassification(
    authorization: string | undefined,
    id: number,
    dto: CreateCatalogItemDto,
  ) {
    this.ensureAdmin(authorization);

    const existing = await this.prisma.classification.findUnique({
      where: { id },
    });
    if (!existing) {
      throw new NotFoundException('La clasificacion no existe');
    }

    try {
      const classification = await this.prisma.classification.update({
        where: { id },
        data: { nombre: dto.nombre.trim() },
      });
      return {
        message: 'Clasificacion actualizada correctamente',
        classification,
      };
    } catch (error) {
      this.throwIfDuplicate(error, 'La clasificacion ya existe');
      throw error;
    }
  }

  async deleteClassification(authorization: string | undefined, id: number) {
    this.ensureAdmin(authorization);

    const existing = await this.prisma.classification.findUnique({
      where: { id },
    });
    if (!existing) {
      throw new NotFoundException('La clasificacion no existe');
    }

    await this.prisma.classification.delete({ where: { id } });

    return {
      message: 'Clasificacion eliminada correctamente',
    };
  }

  private throwIfDuplicate(error: unknown, message: string) {
    if (
      error instanceof Prisma.PrismaClientKnownRequestError &&
      error.code === 'P2002'
    ) {
      throw new BadRequestException(message);
    }
  }

  private ensureAdmin(authorization: string | undefined) {
    const token = extractBearerToken(authorization);
    const payload = verifySessionToken(this.jwtService, token);
    requireAdminRole(payload);
  }
}
