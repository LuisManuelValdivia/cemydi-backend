import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { Prisma, Rol } from '@prisma/client';
import * as bcrypt from 'bcrypt';
import { PrismaService } from '../../prisma/prisma.service';
import {
  extractBearerToken,
  requireAdminRole,
  verifySessionToken,
} from '../auth/session.util';
import { CreateUserDto } from './dto/create-user.dto';
import { UpdateProfileDto } from './dto/update-profile.dto';
import { UpdateUserDto } from './dto/update-user.dto';

@Injectable()
export class UsersService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly jwtService: JwtService,
  ) {}

  async updateMe(authorization: string | undefined, dto: UpdateProfileDto) {
    const payload = this.getSessionPayload(authorization);

    const data: Prisma.UserUpdateInput = {};

    if (dto.nombre !== undefined) data.nombre = dto.nombre.trim();
    if (dto.correo !== undefined) data.correo = dto.correo.trim().toLowerCase();
    if (dto.telefono !== undefined) data.telefono = dto.telefono.trim() || null;
    if (dto.direccion !== undefined)
      data.direccion = dto.direccion.trim() || null;
    if (dto.password !== undefined)
      data.password = await bcrypt.hash(dto.password, 10);

    if (Object.keys(data).length === 0) {
      throw new BadRequestException('No hay campos para actualizar');
    }

    const user = await this.updateUserRecord(payload.sub, data);

    return {
      message: 'Perfil actualizado correctamente',
      user: this.toSafeUser(user),
    };
  }

  async findAll(authorization: string | undefined) {
    this.ensureAdmin(authorization);

    const users = await this.prisma.user.findMany({
      orderBy: { createdAt: 'desc' },
      select: {
        id: true,
        nombre: true,
        correo: true,
        telefono: true,
        direccion: true,
        rol: true,
        activo: true,
        createdAt: true,
      },
    });

    return { users };
  }

  async create(authorization: string | undefined, dto: CreateUserDto) {
    this.ensureAdmin(authorization);

    const existingUser = await this.prisma.user.findUnique({
      where: { correo: dto.correo.trim().toLowerCase() },
    });

    if (existingUser) {
      throw new BadRequestException('El correo ya esta en uso');
    }

    const hashedPassword = await bcrypt.hash(dto.password, 10);

    const user = await this.prisma.user.create({
      data: {
        nombre: dto.nombre.trim(),
        correo: dto.correo.trim().toLowerCase(),
        password: hashedPassword,
        telefono: dto.telefono?.trim() || null,
        direccion: dto.direccion?.trim() || null,
        rol: dto.rol ?? Rol.CLIENT,
        activo: dto.activo ?? true,
      },
    });

    return {
      message: 'Usuario creado correctamente',
      user: this.toSafeUser(user),
    };
  }

  async update(
    authorization: string | undefined,
    id: number,
    dto: UpdateUserDto,
  ) {
    const payload = this.ensureAdmin(authorization);

    const data: Prisma.UserUpdateInput = {};

    if (dto.nombre !== undefined) data.nombre = dto.nombre.trim();
    if (dto.correo !== undefined) data.correo = dto.correo.trim().toLowerCase();
    if (dto.telefono !== undefined) data.telefono = dto.telefono.trim() || null;
    if (dto.direccion !== undefined)
      data.direccion = dto.direccion.trim() || null;
    if (dto.rol !== undefined) data.rol = dto.rol;
    if (dto.password !== undefined)
      data.password = await bcrypt.hash(dto.password, 10);
    if (dto.activo !== undefined) data.activo = dto.activo;

    if (payload.sub === id && dto.activo === false) {
      throw new BadRequestException(
        'No puedes dar de baja tu propio usuario mientras estas en sesion',
      );
    }

    if (Object.keys(data).length === 0) {
      throw new BadRequestException('No hay campos para actualizar');
    }

    const user = await this.updateUserRecord(id, data);

    return {
      message: 'Usuario actualizado correctamente',
      user: this.toSafeUser(user),
    };
  }

  async remove(authorization: string | undefined, id: number) {
    const payload = this.ensureAdmin(authorization);

    if (payload.sub === id) {
      throw new BadRequestException(
        'No puedes eliminar tu propio usuario mientras estas en sesion',
      );
    }

    try {
      await this.prisma.user.delete({ where: { id } });
      return { message: 'Usuario eliminado correctamente' };
    } catch (error) {
      if (
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.code === 'P2025'
      ) {
        throw new NotFoundException('Usuario no encontrado');
      }

      throw error;
    }
  }

  private async updateUserRecord(id: number, data: Prisma.UserUpdateInput) {
    try {
      return await this.prisma.user.update({
        where: { id },
        data,
      });
    } catch (error) {
      if (
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.code === 'P2002'
      ) {
        throw new BadRequestException('El correo ya esta en uso');
      }

      if (
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.code === 'P2025'
      ) {
        throw new NotFoundException('Usuario no encontrado');
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

  private toSafeUser(user: {
    id: number;
    nombre: string;
    correo: string;
    telefono: string | null;
    direccion: string | null;
    rol: Rol;
    activo: boolean;
  }) {
    return {
      id: user.id,
      nombre: user.nombre,
      correo: user.correo,
      telefono: user.telefono,
      direccion: user.direccion,
      rol: user.rol,
      activo: user.activo,
    };
  }
}
