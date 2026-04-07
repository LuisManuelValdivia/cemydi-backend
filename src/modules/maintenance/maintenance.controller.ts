import {
  Body,
  Controller,
  Delete,
  Get,
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
import { MaintenanceService } from './maintenance.service';

@Controller('maintenance')
@Roles(Rol.ADMIN)
@UseGuards(JwtAuthGuard, RolesGuard)
export class MaintenanceController {
  constructor(private readonly maintenanceService: MaintenanceService) {}

  @Get('schedule')
  async getSchedule(@Res() response: Response) {
    const schedule = await this.maintenanceService.getSchedule();
    response.status(200).json({ schedule });
  }

  @Put('schedule')
  async updateSchedule(
    @Body() body: Record<string, unknown>,
    @Res() response: Response,
  ) {
    const schedule = await this.maintenanceService.updateSchedule(body);
    response.status(200).json({
      schedule,
      message:
        'Programacion automatica de mantenimiento actualizada correctamente',
    });
  }

  @Delete('schedule')
  async deleteSchedule(@Res() response: Response) {
    const schedule = await this.maintenanceService.deleteSchedule();
    response.status(200).json({
      schedule,
      message:
        'Programacion automatica de mantenimiento eliminada correctamente',
    });
  }

  @Post('run')
  async runMaintenance(
    @Body() body: Record<string, unknown>,
    @Res() response: Response,
  ) {
    const result = await this.maintenanceService.run(body);
    response.status(200).json(result);
  }
}
