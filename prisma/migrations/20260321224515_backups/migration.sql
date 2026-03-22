-- DropIndex
DROP INDEX "promotions_productId_idx";

-- DropIndex
DROP INDEX "promotions_startAt_endAt_idx";

-- AlterTable
ALTER TABLE "database_backup_schedule" ALTER COLUMN "updatedAt" DROP DEFAULT;

-- AlterTable
ALTER TABLE "products" ADD COLUMN     "activo" BOOLEAN NOT NULL DEFAULT true;

-- AlterTable
ALTER TABLE "promotions" ALTER COLUMN "descripcion" DROP DEFAULT;

-- AlterTable
ALTER TABLE "users" ADD COLUMN     "activo" BOOLEAN NOT NULL DEFAULT true;
