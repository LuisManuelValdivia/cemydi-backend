import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../../prisma/prisma.service';
import { CreateSupplierDto } from './dto/create-supplier.dto';

@Injectable()
export class SuppliersService {
  constructor(private readonly prisma: PrismaService) {}

  async findAll() {
    const suppliers = await this.prisma.supplier.findMany({
      orderBy: { nombre: 'asc' },
    });

    return { suppliers };
  }

  async create(dto: CreateSupplierDto) {
    try {
      const supplier = await this.prisma.supplier.create({
        data: {
          nombre: dto.nombre.trim(),
          encargado: dto.encargado.trim(),
          repartidor: dto.repartidor.trim(),
          direccion: dto.direccion.trim(),
        },
      });

      return {
        message: 'Proveedor creado correctamente',
        supplier,
      };
    } catch (error) {
      if (
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.code === 'P2002'
      ) {
        throw new BadRequestException('El proveedor ya existe');
      }

      throw error;
    }
  }

  async update(id: number, dto: CreateSupplierDto) {
    const existing = await this.prisma.supplier.findUnique({ where: { id } });
    if (!existing) {
      throw new NotFoundException('El proveedor no existe');
    }

    try {
      const supplier = await this.prisma.supplier.update({
        where: { id },
        data: {
          nombre: dto.nombre.trim(),
          encargado: dto.encargado.trim(),
          repartidor: dto.repartidor.trim(),
          direccion: dto.direccion.trim(),
        },
      });

      return {
        message: 'Proveedor actualizado correctamente',
        supplier,
      };
    } catch (error) {
      if (
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.code === 'P2002'
      ) {
        throw new BadRequestException('El proveedor ya existe');
      }

      throw error;
    }
  }

  async delete(id: number) {
    const existing = await this.prisma.supplier.findUnique({ where: { id } });
    if (!existing) {
      throw new NotFoundException('El proveedor no existe');
    }

    await this.prisma.supplier.delete({ where: { id } });

    return {
      message: 'Proveedor eliminado correctamente',
    };
  }
}
