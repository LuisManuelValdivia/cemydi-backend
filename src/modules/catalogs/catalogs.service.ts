import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../../prisma/prisma.service';
import { CreateCatalogItemDto } from './dto/create-catalog-item.dto';

@Injectable()
export class CatalogsService {
  constructor(private readonly prisma: PrismaService) {}

  async findAll() {
    const [brands, classifications] = await Promise.all([
      this.prisma.brand.findMany({ orderBy: { nombre: 'asc' } }),
      this.prisma.classification.findMany({ orderBy: { nombre: 'asc' } }),
    ]);

    return { brands, classifications };
  }

  async createBrand(dto: CreateCatalogItemDto) {
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

  async updateBrand(id: number, dto: CreateCatalogItemDto) {
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

  async deleteBrand(id: number) {
    const existing = await this.prisma.brand.findUnique({ where: { id } });
    if (!existing) {
      throw new NotFoundException('La marca no existe');
    }

    await this.prisma.brand.delete({ where: { id } });

    return {
      message: 'Marca eliminada correctamente',
    };
  }

  async createClassification(dto: CreateCatalogItemDto) {
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

  async updateClassification(id: number, dto: CreateCatalogItemDto) {
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

  async deleteClassification(id: number) {
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
}
