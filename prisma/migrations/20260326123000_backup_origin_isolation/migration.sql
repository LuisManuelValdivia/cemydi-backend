-- CreateEnum
CREATE TYPE "BackupOrigin" AS ENUM ('MANUAL', 'AUTOMATIC', 'TABLE');

-- AlterTable
ALTER TABLE "database_backups"
ADD COLUMN "origin" "BackupOrigin" NOT NULL DEFAULT 'MANUAL';

-- CreateIndex
CREATE INDEX "database_backups_origin_createdAt_idx"
ON "database_backups"("origin", "createdAt");
