ALTER TABLE "database_backups"
ADD COLUMN "driveFileId" TEXT;

CREATE UNIQUE INDEX "database_backups_driveFileId_key"
ON "database_backups"("driveFileId");

ALTER TABLE "database_backups"
ALTER COLUMN "content" DROP NOT NULL;
