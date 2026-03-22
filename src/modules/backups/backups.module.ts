import { Module } from '@nestjs/common';
import { PrismaModule } from '../../prisma/prisma.module';
import { BackupsController } from './backups.controller';
import { BackupsService } from './backups.service';

@Module({
  imports: [PrismaModule],
  controllers: [BackupsController],
  providers: [BackupsService],
})
export class BackupsModule {}
