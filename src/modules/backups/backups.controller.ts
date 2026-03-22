import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  ParseIntPipe,
  Post,
  Put,
  Res,
  UseGuards,
} from '@nestjs/common';
import { Rol } from '@prisma/client';
import type { Response } from 'express';
import { Roles } from '../auth/decorators/roles.decorator';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { RolesGuard } from '../auth/guards/roles.guard';
import { BackupsService } from './backups.service';

@Controller('backups')
@Roles(Rol.ADMIN)
@UseGuards(JwtAuthGuard, RolesGuard)
export class BackupsController {
  constructor(private readonly backupsService: BackupsService) {}

  @Get('database/status')
  async getDatabaseStatus(@Res() response: Response) {
    const status = await this.backupsService.getDatabaseStatus();
    response.status(200).json({ status });
  }

  @Get('database/history')
  async listDatabaseBackups(@Res() response: Response) {
    const backups = await this.backupsService.listDatabaseBackupRecords();
    response.status(200).json({ backups });
  }

  @Get('database/schedule')
  async getDatabaseBackupSchedule(@Res() response: Response) {
    const schedule = await this.backupsService.getDatabaseBackupSchedule();
    response.status(200).json({ schedule });
  }

  @Put('database/schedule')
  async updateDatabaseBackupSchedule(
    @Body() body: Record<string, unknown>,
    @Res() response: Response,
  ) {
    const schedule = await this.backupsService.updateDatabaseBackupSchedule(body);
    response.status(200).json({
      schedule,
      message: 'Programacion de respaldos actualizada correctamente',
    });
  }

  @Delete('database/schedule')
  async deleteDatabaseBackupSchedule(@Res() response: Response) {
    const schedule = await this.backupsService.deleteDatabaseBackupSchedule();
    response.status(200).json({
      schedule,
      message: 'Programacion automatica eliminada correctamente',
    });
  }

  @Post('database')
  async createDatabaseBackup(@Res() response: Response) {
    const backup = await this.backupsService.createDatabaseBackupRecord();
    response.status(201).json({
      backup,
      message: 'Respaldo generado y registrado correctamente',
    });
  }

  @Post('database/table')
  async createSingleTableBackup(
    @Body('tableName') tableName: string,
    @Res() response: Response,
  ) {
    const backup = await this.backupsService.createSingleTableBackupRecord(tableName);
    response.status(201).json({
      backup,
      message: 'Respaldo de tabla generado y registrado correctamente',
    });
  }

  @Get('database/:id/download')
  async downloadDatabaseBackupById(
    @Param('id', ParseIntPipe) id: number,
    @Res() response: Response,
  ) {
    const backup = await this.backupsService.getDatabaseBackupRecord(id);

    response.setHeader('Content-Type', 'application/x-tar');
    response.setHeader(
      'Content-Disposition',
      `attachment; filename="${backup.fileName}"`,
    );
    response.status(200).send(backup.content);
  }

  @Get('database')
  async downloadDatabaseBackup(@Res() response: Response) {
    const backup = await this.backupsService.createDatabaseBackup();

    response.setHeader('Content-Type', 'application/x-tar');
    response.setHeader(
      'Content-Disposition',
      `attachment; filename="${backup.fileName}"`,
    );
    response.status(200).send(backup.content);
  }

  @Delete('database/:id')
  async deleteDatabaseBackupRecord(
    @Param('id', ParseIntPipe) id: number,
    @Res() response: Response,
  ) {
    const backup = await this.backupsService.deleteDatabaseBackupRecord(id);
    response
      .status(200)
      .json({ backup, message: 'Respaldo eliminado correctamente' });
  }
}
