import {
  BadRequestException,
  Injectable,
  InternalServerErrorException,
  Logger,
  NotFoundException,
  UnauthorizedException,
} from '@nestjs/common';
import { createHash, randomUUID } from 'node:crypto';
import { Prisma, TipoAdquisicion } from '@prisma/client';
import { PrismaService } from '../../prisma/prisma.service';
import type { AuthUser } from '../auth/auth-user.interface';
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

const productInclude = {
  images: {
    orderBy: {
      sortOrder: 'asc',
    },
  },
} satisfies Prisma.ProductInclude;

type ProductWithImages = Prisma.ProductGetPayload<{
  include: typeof productInclude;
}>;

type UploadedProductImage = {
  imageUrl: string;
  cloudinaryPublicId: string | null;
};

type CloudinaryCredentials = {
  cloudName: string;
  apiKey: string;
  apiSecret: string;
};

type UploadedProductFile = {
  originalname: string;
  mimetype: string;
  size: number;
  buffer: Buffer;
};

function normalizeClassificationName(value: string) {
  return value
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\uFFFD/g, '')
    .replace(/[^a-z0-9\s]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function levenshteinDistance(a: string, b: string) {
  const rows = a.length + 1;
  const cols = b.length + 1;
  const dp = Array.from({ length: rows }, () => Array<number>(cols).fill(0));

  for (let i = 0; i < rows; i += 1) dp[i][0] = i;
  for (let j = 0; j < cols; j += 1) dp[0][j] = j;

  for (let i = 1; i < rows; i += 1) {
    for (let j = 1; j < cols; j += 1) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      dp[i][j] = Math.min(
        dp[i - 1][j] + 1,
        dp[i][j - 1] + 1,
        dp[i - 1][j - 1] + cost,
      );
    }
  }

  return dp[a.length][b.length];
}

function areEquivalentClassifications(a: string, b: string) {
  const normalizedA = normalizeClassificationName(a);
  const normalizedB = normalizeClassificationName(b);

  if (!normalizedA || !normalizedB) return false;
  if (normalizedA === normalizedB) return true;
  if (Math.abs(normalizedA.length - normalizedB.length) > 2) return false;

  return levenshteinDistance(normalizedA, normalizedB) <= 1;
}

function canonicalizeClassification(value: string) {
  const trimmed = value.trim();
  const normalized = normalizeClassificationName(trimmed);

  if (
    normalized === 'equipomedico' ||
    normalized === 'equipomdico' ||
    (normalized.startsWith('equipo m') && normalized.endsWith('dico'))
  ) {
    return 'Equipo Medico';
  }

  return trimmed.replace(/\uFFFD/g, '');
}

@Injectable()
export class ProductsService {
  private readonly logger = new Logger(ProductsService.name);

  constructor(private readonly prisma: PrismaService) {}

  async findAll(query: FindProductsQuery, user?: AuthUser) {
    const db = this.prisma.forUser(user);
    const andFilters: Prisma.ProductWhereInput[] = [];
    const validTipos = query.tipos.filter(
      (value): value is TipoAdquisicion =>
        value === 'VENTA' || value === 'RENTA' || value === 'MIXTO',
    );

    if (query.includeInactive) {
      this.ensureAdmin(user);
    } else {
      andFilters.push({ activo: true });
    }

    if (query.clasificaciones.length > 0) {
      const productClassificationRows = await db.product.findMany({
        where: query.includeInactive ? {} : { activo: true },
        select: { clasificacion: true },
        distinct: ['clasificacion'],
      });

      const matchedClassifications = productClassificationRows
        .map((item) => item.clasificacion.trim())
        .filter((value) =>
          query.clasificaciones.some((selected) =>
            areEquivalentClassifications(value, selected),
          ),
        );

      andFilters.push({
        clasificacion: {
          in:
            matchedClassifications.length > 0
              ? matchedClassifications
              : query.clasificaciones,
        },
      });
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

    let products: ProductWithImages[] = [];
    let total = 0;
    let page = 1;
    let totalPages = 1;

    if (paginationRequested) {
      total = await db.product.count({ where });
      totalPages = Math.max(1, Math.ceil(total / pageSize));
      page = Math.min(requestedPage, totalPages);
      const skip = (page - 1) * pageSize;

      products = await db.product.findMany({
        where,
        include: productInclude,
        orderBy: { createdAt: 'desc' },
        skip,
        take: pageSize,
      });
    } else {
      products = await db.product.findMany({
        where,
        include: productInclude,
        orderBy: { createdAt: 'desc' },
      });
      total = products.length;
      totalPages = 1;
      page = 1;
    }

    const [productClassifications, catalogClassifications] = await Promise.all([
      db.product.findMany({
        where: query.includeInactive ? {} : { activo: true },
        select: { clasificacion: true },
        distinct: ['clasificacion'],
        orderBy: { clasificacion: 'asc' },
      }),
      db.classification.findMany({
        select: { nombre: true },
        orderBy: { nombre: 'asc' },
      }),
    ]);

    const clasificaciones: string[] = [];

    for (const value of [
      ...catalogClassifications.map((item) => item.nombre.trim()),
      ...productClassifications.map((item) => item.clasificacion.trim()),
    ].filter(Boolean)) {
      const existingIndex = clasificaciones.findIndex((item) =>
        areEquivalentClassifications(item, value),
      );

      if (existingIndex === -1) {
        clasificaciones.push(value);
        continue;
      }

      if (
        !clasificaciones[existingIndex].includes('\uFFFD') &&
        value.includes('\uFFFD')
      ) {
        continue;
      }

      if (
        clasificaciones[existingIndex].includes('\uFFFD') &&
        !value.includes('\uFFFD')
      ) {
        clasificaciones[existingIndex] = value;
      }
    }

    clasificaciones.sort((a, b) =>
      a.localeCompare(b, 'es', { sensitivity: 'base' }),
    );

    return {
      products: products.map((product) => this.mapProduct(product)),
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

  async findOne(id: number, includeInactive: boolean, user?: AuthUser) {
    const db = this.prisma.forUser(user);
    const where: Prisma.ProductWhereInput = { id };

    if (includeInactive) {
      this.ensureAdmin(user);
    } else {
      where.activo = true;
    }

    const product = await db.product.findFirst({
      where,
      include: productInclude,
    });

    if (!product) {
      throw new NotFoundException('Producto no encontrado');
    }

    return { product: this.mapProduct(product) };
  }

  async create(dto: CreateProductDto, files: UploadedProductFile[] = []) {
    this.validateImageOperation(dto.imageUrls ?? [], files);
    const uploadedImages = await this.uploadProductImages(
      dto.imageUrls ?? [],
      files,
    );

    try {
      const product = await this.prisma.product.create({
        data: {
          nombre: dto.nombre.trim(),
          marca: dto.marca.trim(),
          modelo: dto.modelo.trim(),
          descripcion: dto.descripcion.trim(),
          precio: dto.precio,
          clasificacion: canonicalizeClassification(dto.clasificacion),
          stock: dto.stock,
          proveedor: dto.proveedor.trim(),
          tipoAdquisicion: dto.tipoAdquisicion,
          requiereReceta: dto.requiereReceta ?? false,
          activo: dto.activo ?? true,
          images: uploadedImages.length
            ? {
                create: uploadedImages.map((image, index) => ({
                  imageUrl: image.imageUrl,
                  cloudinaryPublicId: image.cloudinaryPublicId,
                  sortOrder: index,
                })),
              }
            : undefined,
        },
        include: productInclude,
      });

      return {
        message: 'Producto creado correctamente',
        product: this.mapProduct(product),
      };
    } catch (error) {
      await this.deleteUploadedImagesQuietly(uploadedImages);
      throw error;
    }
  }

  async update(
    id: number,
    dto: UpdateProductDto,
    files: UploadedProductFile[] = [],
  ) {
    const existing = await this.prisma.product.findUnique({
      where: { id },
      include: productInclude,
    });

    if (!existing) {
      throw new NotFoundException('Producto no encontrado');
    }

    this.validateImageOperation(dto.imageUrls ?? [], files);

    const data: Prisma.ProductUpdateInput = {};

    if (dto.nombre !== undefined) data.nombre = dto.nombre.trim();
    if (dto.marca !== undefined) data.marca = dto.marca.trim();
    if (dto.modelo !== undefined) data.modelo = dto.modelo.trim();
    if (dto.descripcion !== undefined) {
      data.descripcion = dto.descripcion.trim();
    }
    if (dto.precio !== undefined) data.precio = dto.precio;
    if (dto.clasificacion !== undefined) {
      data.clasificacion = canonicalizeClassification(dto.clasificacion);
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

    const wantsImageSync =
      dto.keepImageIds !== undefined ||
      (dto.imageUrls?.length ?? 0) > 0 ||
      files.length > 0;

    if (Object.keys(data).length === 0 && !wantsImageSync) {
      throw new BadRequestException('No hay campos para actualizar');
    }

    const keepImageIdsSet = new Set(
      dto.keepImageIds ?? existing.images.map((image) => image.id),
    );

    const invalidKeepIds = [...keepImageIdsSet].filter(
      (imageId) => !existing.images.some((image) => image.id === imageId),
    );
    if (invalidKeepIds.length > 0) {
      throw new BadRequestException(
        'La galeria contiene imagenes no asociadas a este producto',
      );
    }

    const keptImages = wantsImageSync
      ? existing.images.filter((image) => keepImageIdsSet.has(image.id))
      : existing.images;
    const totalImagesAfterUpdate =
      keptImages.length + (dto.imageUrls?.length ?? 0) + files.length;
    if (totalImagesAfterUpdate > 10) {
      throw new BadRequestException(
        'Solo se permiten hasta 10 imagenes por producto',
      );
    }

    const uploadedImages = wantsImageSync
      ? await this.uploadProductImages(dto.imageUrls ?? [], files)
      : [];
    const removedImages = wantsImageSync
      ? existing.images.filter((image) => !keepImageIdsSet.has(image.id))
      : [];

    try {
      const product = await this.prisma.$transaction(async (tx) => {
        await tx.product.update({
          where: { id },
          data,
        });

        if (wantsImageSync) {
          if (keptImages.length > 0) {
            await Promise.all(
              keptImages.map((image, index) =>
                tx.productImage.update({
                  where: { id: image.id },
                  data: { sortOrder: index },
                }),
              ),
            );
          }

          if (removedImages.length > 0) {
            await tx.productImage.deleteMany({
              where: {
                id: { in: removedImages.map((image) => image.id) },
              },
            });
          }

          if (uploadedImages.length > 0) {
            await tx.productImage.createMany({
              data: uploadedImages.map((image, index) => ({
                productId: id,
                imageUrl: image.imageUrl,
                cloudinaryPublicId: image.cloudinaryPublicId,
                sortOrder: keptImages.length + index,
              })),
            });
          }
        }

        return tx.product.findUniqueOrThrow({
          where: { id },
          include: productInclude,
        });
      });

      await this.deleteCloudinaryImagesQuietly(removedImages);

      return {
        message: 'Producto actualizado correctamente',
        product: this.mapProduct(product),
      };
    } catch (error) {
      await this.deleteUploadedImagesQuietly(uploadedImages);

      if (
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.code === 'P2025'
      ) {
        throw new NotFoundException('Producto no encontrado');
      }

      throw error;
    }
  }

  async remove(id: number) {
    const product = await this.prisma.product.findUnique({
      where: { id },
      include: productInclude,
    });

    if (!product) {
      throw new NotFoundException('Producto no encontrado');
    }

    await this.prisma.product.delete({ where: { id } });
    await this.deleteCloudinaryImagesQuietly(product.images);

    return { message: 'Producto eliminado correctamente' };
  }

  private mapProduct(product: ProductWithImages) {
    const images = product.images.map((image) => ({
      id: image.id,
      imageUrl: image.imageUrl,
      sortOrder: image.sortOrder,
      createdAt: image.createdAt.toISOString(),
    }));

    return {
      ...product,
      clasificacion: canonicalizeClassification(product.clasificacion),
      createdAt: product.createdAt.toISOString(),
      imageUrl: images[0]?.imageUrl ?? null,
      images,
    };
  }

  private validateImageOperation(
    imageUrls: string[],
    files: UploadedProductFile[],
  ) {
    const total = imageUrls.length + files.length;
    if (total > 10) {
      throw new BadRequestException(
        'Solo se permiten hasta 10 imagenes por producto',
      );
    }

    for (const file of files) {
      if (!file.mimetype.startsWith('image/')) {
        throw new BadRequestException(
          'Solo se permiten archivos de imagen para los productos',
        );
      }

      if (file.size > 8 * 1024 * 1024) {
        throw new BadRequestException(
          'Cada imagen debe pesar como maximo 8 MB',
        );
      }
    }
  }

  private async uploadProductImages(
    imageUrls: string[],
    files: UploadedProductFile[],
  ) {
    const uploaded: UploadedProductImage[] = [];

    try {
      for (const imageUrl of imageUrls) {
        uploaded.push(await this.uploadRemoteImageToCloudinary(imageUrl));
      }

      for (const file of files) {
        uploaded.push(await this.uploadFileToCloudinary(file));
      }

      return uploaded;
    } catch (error) {
      await this.deleteUploadedImagesQuietly(uploaded);
      throw error;
    }
  }

  private async uploadRemoteImageToCloudinary(imageUrl: string) {
    try {
      const parsed = new URL(imageUrl);
      if (!['http:', 'https:'].includes(parsed.protocol)) {
        throw new Error('La URL debe usar http o https');
      }
    } catch (error) {
      const message =
        error instanceof Error ? error.message : 'URL de imagen invalida';
      throw new BadRequestException(`URL de imagen invalida: ${message}`);
    }

    return this.uploadToCloudinary(imageUrl);
  }

  private async uploadFileToCloudinary(file: UploadedProductFile) {
    const bytes = new Uint8Array(file.buffer);
    const blob = new Blob([bytes], { type: file.mimetype });
    return this.uploadToCloudinary(blob, file.originalname);
  }

  private async uploadToCloudinary(file: string | Blob, fileName?: string) {
    const credentials = this.getCloudinaryCredentials();
    const timestamp = Math.floor(Date.now() / 1000);
    const folder = this.getCloudinaryProductFolder();
    const publicId = randomUUID();
    const normalizedFileName = fileName?.trim() || '';
    const signature = this.signCloudinaryParams({
      filename_override: normalizedFileName,
      folder,
      public_id: publicId,
      timestamp: String(timestamp),
    });

    const form = new FormData();
    form.set('file', file);
    form.set('api_key', credentials.apiKey);
    form.set('timestamp', String(timestamp));
    form.set('folder', folder);
    form.set('public_id', publicId);
    form.set('signature', signature);
    if (normalizedFileName) {
      form.set('filename_override', normalizedFileName);
    }

    const response = await fetch(
      `https://api.cloudinary.com/v1_1/${credentials.cloudName}/image/upload`,
      {
        method: 'POST',
        body: form,
      },
    );

    const payload = (await response.json()) as {
      secure_url?: string;
      public_id?: string;
      error?: { message?: string };
    };

    if (!response.ok || !payload.secure_url) {
      const message =
        payload.error?.message || 'Cloudinary rechazo la imagen del producto';
      throw new InternalServerErrorException(
        `No se pudo subir la imagen del producto: ${message}`,
      );
    }

    return {
      imageUrl: payload.secure_url,
      cloudinaryPublicId: payload.public_id ?? null,
    };
  }

  private async deleteCloudinaryImage(publicId: string) {
    const credentials = this.getCloudinaryCredentials();
    const timestamp = Math.floor(Date.now() / 1000);
    const signature = this.signCloudinaryParams({
      invalidate: 'true',
      public_id: publicId,
      timestamp: String(timestamp),
    });

    const form = new FormData();
    form.set('api_key', credentials.apiKey);
    form.set('invalidate', 'true');
    form.set('public_id', publicId);
    form.set('timestamp', String(timestamp));
    form.set('signature', signature);

    const response = await fetch(
      `https://api.cloudinary.com/v1_1/${credentials.cloudName}/image/destroy`,
      {
        method: 'POST',
        body: form,
      },
    );

    const payload = (await response.json().catch(() => null)) as {
      result?: string;
      error?: { message?: string };
    } | null;

    if (!response.ok) {
      throw new Error(
        payload?.error?.message ||
          `Cloudinary respondio con estado ${response.status}`,
      );
    }

    if (payload?.result && !['ok', 'not found'].includes(payload.result)) {
      throw new Error(
        `Cloudinary no confirmo la eliminacion de la imagen: ${payload.result}`,
      );
    }
  }

  private async deleteUploadedImagesQuietly(images: UploadedProductImage[]) {
    await Promise.all(
      images
        .filter((image) => image.cloudinaryPublicId)
        .map((image) =>
          this.deleteCloudinaryImage(image.cloudinaryPublicId!).catch(
            (error) => {
              const message =
                error instanceof Error
                  ? error.message
                  : 'Error desconocido al limpiar imagen de Cloudinary';
              this.logger.warn(
                `No se pudo revertir la imagen subida del producto: ${message}`,
              );
            },
          ),
        ),
    );
  }

  private async deleteCloudinaryImagesQuietly(
    images: Array<{ cloudinaryPublicId: string | null }>,
  ) {
    await Promise.all(
      images
        .filter((image) => image.cloudinaryPublicId)
        .map((image) =>
          this.deleteCloudinaryImage(image.cloudinaryPublicId!).catch(
            (error) => {
              const message =
                error instanceof Error
                  ? error.message
                  : 'Error desconocido al eliminar imagen de Cloudinary';
              this.logger.warn(
                `No se pudo eliminar una imagen de producto en Cloudinary: ${message}`,
              );
            },
          ),
        ),
    );
  }

  private async deleteCloudinaryImagesStrictly(
    images: Array<{ cloudinaryPublicId: string | null }>,
  ) {
    const publicIds = images
      .map((image) => image.cloudinaryPublicId)
      .filter((publicId): publicId is string => Boolean(publicId));

    for (const publicId of publicIds) {
      try {
        await this.deleteCloudinaryImage(publicId);
      } catch (error) {
        const message =
          error instanceof Error
            ? error.message
            : 'Error desconocido al eliminar imagen de Cloudinary';
        this.logger.error(
          `No se pudo eliminar la imagen ${publicId} de Cloudinary: ${message}`,
        );
        throw new InternalServerErrorException(
          `No se pudo eliminar la imagen del producto en Cloudinary: ${message}`,
        );
      }
    }
  }

  private getCloudinaryCredentials(): CloudinaryCredentials {
    const cloudinaryUrl = process.env.CLOUDINARY_URL?.trim();
    if (!cloudinaryUrl) {
      throw new InternalServerErrorException(
        'Configura CLOUDINARY_URL para administrar imagenes de productos',
      );
    }

    let parsed: URL;
    try {
      parsed = new URL(cloudinaryUrl);
    } catch {
      throw new InternalServerErrorException(
        'CLOUDINARY_URL no tiene un formato valido',
      );
    }

    if (parsed.protocol !== 'cloudinary:') {
      throw new InternalServerErrorException(
        'CLOUDINARY_URL debe iniciar con cloudinary://',
      );
    }

    const apiKey = decodeURIComponent(parsed.username).replace(/[<>]/g, '');
    const apiSecret = decodeURIComponent(parsed.password).replace(/[<>]/g, '');
    const cloudName = parsed.hostname.trim();

    if (!apiKey || !apiSecret || !cloudName) {
      throw new InternalServerErrorException(
        'CLOUDINARY_URL debe incluir cloud name, api key y api secret',
      );
    }

    return {
      cloudName,
      apiKey,
      apiSecret,
    };
  }

  private getCloudinaryProductFolder() {
    const configured = process.env.CLOUDINARY_PRODUCTS_FOLDER?.trim();
    return configured || 'cemydi/products';
  }

  private signCloudinaryParams(params: Record<string, string>) {
    const credentials = this.getCloudinaryCredentials();
    const payload = Object.entries(params)
      .filter(([, value]) => value !== '')
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, value]) => `${key}=${value}`)
      .join('&');

    return createHash('sha1')
      .update(`${payload}${credentials.apiSecret}`)
      .digest('hex');
  }

  private ensureAdmin(user?: AuthUser) {
    if (!user) {
      throw new UnauthorizedException('No autenticado');
    }

    if (user.rol !== 'ADMIN') {
      throw new UnauthorizedException('No tienes permisos de administrador');
    }
  }
}
