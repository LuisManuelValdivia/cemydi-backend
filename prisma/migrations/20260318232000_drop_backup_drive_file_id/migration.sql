DROP INDEX IF EXISTS "database_backups_driveFileId_key";

ALTER TABLE "database_backups"
DROP COLUMN IF EXISTS "driveFileId";
