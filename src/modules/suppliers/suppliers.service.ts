import { BadRequestException, Injectable } from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../../prisma/prisma.service';
import {
  extractBearerToken,
  requireAdminRole,
  verifySessionToken,
} from '../auth/session.util';
import { CreateSupplierDto } from './dto/create-supplier.dto';

@Injectable()
export class SuppliersService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly jwtService: JwtService,
  ) {}

  async findAll(authorization: string | undefined) {
    this.ensureAdmin(authorization);

    const suppliers = await this.prisma.supplier.findMany({
      orderBy: { nombre: 'asc' },
    });

    return { suppliers };
  }

  async create(authorization: string | undefined, dto: CreateSupplierDto) {
    this.ensureAdmin(authorization);

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

  private ensureAdmin(authorization: string | undefined) {
    const token = extractBearerToken(authorization);
    const payload = verifySessionToken(this.jwtService, token);
    requireAdminRole(payload);
  }
}
