import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { type Product, Prisma, TipoAdquisicion } from '@prisma/client';
import { PrismaService } from '../../prisma/prisma.service';
import {
  extractBearerToken,
  requireAdminRole,
  verifySessionToken,
} from '../auth/session.util';
import { CreateProductDto } from './dto/create-product.dto';
import { UpdateProductDto } from './dto/update-product.dto';

type FindProductsQuery = {
  search?: string;
  clasificaciones: string[];
  tipos: string[];
  requiereRecetaRaw?: string;
  includeInactive: boolean;
  pageRaw?: string;
  pageSizeRaw?: string;
};

@Injectable()
export class ProductsService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly jwtService: JwtService,
  ) {}

  async findAll(query: FindProductsQuery, authorization: string | undefined) {
    const andFilters: Prisma.ProductWhereInput[] = [];
    const validTipos = query.tipos.filter(
      (value): value is TipoAdquisicion =>
        value === 'VENTA' || value === 'RENTA' || value === 'MIXTO',
    );

    if (query.includeInactive) {
      this.ensureAdmin(authorization);
    } else {
      andFilters.push({ activo: true });
    }

    if (query.clasificaciones.length > 0) {
      andFilters.push({ clasificacion: { in: query.clasificaciones } });
    }

    if (validTipos.length > 0) {
      andFilters.push({ tipoAdquisicion: { in: validTipos } });
    }

    if (query.requiereRecetaRaw === 'true') {
      andFilters.push({ requiereReceta: true });
    } else if (query.requiereRecetaRaw === 'false') {
      andFilters.push({ requiereReceta: false });
    }

    if (query.search?.trim()) {
      const term = query.search.trim();
      andFilters.push({
        OR: [
          { nombre: { contains: term, mode: 'insensitive' } },
          { marca: { contains: term, mode: 'insensitive' } },
          { modelo: { contains: term, mode: 'insensitive' } },
          { clasificacion: { contains: term, mode: 'insensitive' } },
          { proveedor: { contains: term, mode: 'insensitive' } },
          { descripcion: { contains: term, mode: 'insensitive' } },
        ],
      });
    }

    const where: Prisma.ProductWhereInput =
      andFilters.length > 0 ? { AND: andFilters } : {};

    const parsedPage = Number(query.pageRaw ?? '');
    const parsedPageSize = Number(query.pageSizeRaw ?? '');
    const paginationRequested =
      query.pageRaw !== undefined || query.pageSizeRaw !== undefined;
    const pageSize =
      Number.isInteger(parsedPageSize) && parsedPageSize > 0
        ? Math.min(parsedPageSize, 60)
        : 9;
    const requestedPage =
      Number.isInteger(parsedPage) && parsedPage > 0 ? parsedPage : 1;

    let products: Product[] = [];
    let total = 0;
    let page = 1;
    let totalPages = 1;

    if (paginationRequested) {
      total = await this.prisma.product.count({ where });
      totalPages = Math.max(1, Math.ceil(total / pageSize));
      page = Math.min(requestedPage, totalPages);
      const skip = (page - 1) * pageSize;

      products = await this.prisma.product.findMany({
        where,
        orderBy: { createdAt: 'desc' },
        skip,
        take: pageSize,
      });
    } else {
      products = await this.prisma.product.findMany({
        where,
        orderBy: { createdAt: 'desc' },
      });
      total = products.length;
      totalPages = 1;
      page = 1;
    }

    const [productClassifications, catalogClassifications] = await Promise.all([
      this.prisma.product.findMany({
        where: query.includeInactive ? {} : { activo: true },
        select: { clasificacion: true },
        distinct: ['clasificacion'],
        orderBy: { clasificacion: 'asc' },
      }),
      this.prisma.classification.findMany({
        select: { nombre: true },
        orderBy: { nombre: 'asc' },
      }),
    ]);

    const clasificaciones = Array.from(
      new Set(
        [
          ...catalogClassifications.map((item) => item.nombre.trim()),
          ...productClassifications.map((item) => item.clasificacion.trim()),
        ].filter(Boolean),
      ),
    ).sort((a, b) => a.localeCompare(b, 'es', { sensitivity: 'base' }));

    return {
      products,
      filters: {
        clasificaciones,
      },
      pagination: {
        page,
        pageSize: paginationRequested ? pageSize : products.length || 1,
        total,
        totalPages,
        hasPrevious: page > 1,
        hasNext: page < totalPages,
      },
    };
  }

  async findOne(
    id: number,
    includeInactive: boolean,
    authorization: string | undefined,
  ) {
    const where: Prisma.ProductWhereInput = { id };

    if (includeInactive) {
      this.ensureAdmin(authorization);
    } else {
      where.activo = true;
    }

    const product = await this.prisma.product.findFirst({ where });

    if (!product) {
      throw new NotFoundException('Producto no encontrado');
    }

    return { product };
  }

  async create(authorization: string | undefined, dto: CreateProductDto) {
    this.ensureAdmin(authorization);

    const product = await this.prisma.product.create({
      data: {
        nombre: dto.nombre.trim(),
        marca: dto.marca.trim(),
        modelo: dto.modelo.trim(),
        descripcion: dto.descripcion.trim(),
        precio: dto.precio,
        clasificacion: dto.clasificacion.trim(),
        stock: dto.stock,
        proveedor: dto.proveedor.trim(),
        tipoAdquisicion: dto.tipoAdquisicion,
        requiereReceta: dto.requiereReceta ?? false,
        activo: dto.activo ?? true,
      },
    });

    return {
      message: 'Producto creado correctamente',
      product,
    };
  }

  async update(
    authorization: string | undefined,
    id: number,
    dto: UpdateProductDto,
  ) {
    this.ensureAdmin(authorization);

    const data: Prisma.ProductUpdateInput = {};

    if (dto.nombre !== undefined) data.nombre = dto.nombre.trim();
    if (dto.marca !== undefined) data.marca = dto.marca.trim();
    if (dto.modelo !== undefined) data.modelo = dto.modelo.trim();
    if (dto.descripcion !== undefined)
      data.descripcion = dto.descripcion.trim();
    if (dto.precio !== undefined) data.precio = dto.precio;
    if (dto.clasificacion !== undefined) {
      data.clasificacion = dto.clasificacion.trim();
    }
    if (dto.stock !== undefined) data.stock = dto.stock;
    if (dto.proveedor !== undefined) data.proveedor = dto.proveedor.trim();
    if (dto.tipoAdquisicion !== undefined) {
      data.tipoAdquisicion = dto.tipoAdquisicion;
    }
    if (dto.requiereReceta !== undefined) {
      data.requiereReceta = dto.requiereReceta;
    }
    if (dto.activo !== undefined) {
      data.activo = dto.activo;
    }

    if (Object.keys(data).length === 0) {
      throw new BadRequestException('No hay campos para actualizar');
    }

    try {
      const product = await this.prisma.product.update({
        where: { id },
        data,
      });

      return {
        message: 'Producto actualizado correctamente',
        product,
      };
    } catch (error) {
      if (
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.code === 'P2025'
      ) {
        throw new NotFoundException('Producto no encontrado');
      }

      throw error;
    }
  }

  async remove(authorization: string | undefined, id: number) {
    this.ensureAdmin(authorization);

    try {
      await this.prisma.product.delete({ where: { id } });
      return { message: 'Producto eliminado correctamente' };
    } catch (error) {
      if (
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.code === 'P2025'
      ) {
        throw new NotFoundException('Producto no encontrado');
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
