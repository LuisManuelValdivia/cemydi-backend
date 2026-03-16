import {
  Body,
  Controller,
  Delete,
  Get,
  Headers,
  Param,
  ParseIntPipe,
  Post,
  Res,
} from '@nestjs/common';
import type { Response } from 'express';
import { BackupsService } from './backups.service';

@Controller('backups')
export class BackupsController {
  constructor(private readonly backupsService: BackupsService) {}

  @Get('database/status')
  async getDatabaseStatus(
    @Headers('authorization') authorization: string | undefined,
    @Res() response: Response,
  ) {
    const status = await this.backupsService.getDatabaseStatus(authorization);
    response.status(200).json({ status });
  }

  @Get('database/history')
  async listDatabaseBackups(
    @Headers('authorization') authorization: string | undefined,
    @Res() response: Response,
  ) {
    const backups = await this.backupsService.listDatabaseBackupRecords(authorization);
    response.status(200).json({ backups });
  }

  @Post('database')
  async createDatabaseBackup(
    @Headers('authorization') authorization: string | undefined,
    @Res() response: Response,
  ) {
    const backup = await this.backupsService.createDatabaseBackupRecord(authorization);
    response
      .status(201)
      .json({ backup, message: 'Respaldo generado y registrado correctamente' });
  }

  @Post('database/table')
  async createSingleTableBackup(
    @Headers('authorization') authorization: string | undefined,
    @Body('tableName') tableName: string,
    @Res() response: Response,
  ) {
    const backup = await this.backupsService.createSingleTableBackupRecord(
      authorization,
      tableName,
    );
    response
      .status(201)
      .json({ backup, message: 'Respaldo de tabla generado y registrado correctamente' });
  }

  @Get('database/:id/download')
  async downloadDatabaseBackupById(
    @Headers('authorization') authorization: string | undefined,
    @Param('id', ParseIntPipe) id: number,
    @Res() response: Response,
  ) {
    const backup = await this.backupsService.getDatabaseBackupRecord(authorization, id);

    response.setHeader('Content-Type', 'application/x-tar');
    response.setHeader(
      'Content-Disposition',
      `attachment; filename="${backup.fileName}"`,
    );
    response.status(200).send(backup.content);
  }

  @Get('database')
  async downloadDatabaseBackup(
    @Headers('authorization') authorization: string | undefined,
    @Res() response: Response,
  ) {
    const backup = await this.backupsService.createDatabaseBackup(authorization);

    response.setHeader('Content-Type', 'application/x-tar');
    response.setHeader(
      'Content-Disposition',
      `attachment; filename="${backup.fileName}"`,
    );
    response.status(200).send(backup.content);
  }

  @Delete('database/:id')
  async deleteDatabaseBackupRecord(
    @Headers('authorization') authorization: string | undefined,
    @Param('id', ParseIntPipe) id: number,
    @Res() response: Response,
  ) {
    const backup = await this.backupsService.deleteDatabaseBackupRecord(authorization, id);
    response.status(200).json({ backup, message: 'Respaldo eliminado correctamente' });
  }
}
