CREATE TABLE "management"."database_maintenance_schedule" (
  "id" INTEGER NOT NULL DEFAULT 1,
  "isEnabled" BOOLEAN NOT NULL DEFAULT false,
  "intervalDays" INTEGER NOT NULL DEFAULT 1,
  "runAtTime" TEXT NOT NULL DEFAULT '04:00',
  "operation" TEXT NOT NULL DEFAULT 'VACUUM_ANALYZE',
  "tableName" TEXT,
  "lastRunAt" TIMESTAMP(3),
  "nextRunAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT "database_maintenance_schedule_pkey" PRIMARY KEY ("id")
);

INSERT INTO "management"."database_maintenance_schedule" (
  "id",
  "isEnabled",
  "intervalDays",
  "runAtTime",
  "operation",
  "tableName",
  "lastRunAt",
  "nextRunAt",
  "createdAt",
  "updatedAt"
)
VALUES (
  1,
  false,
  1,
  '04:00',
  'VACUUM_ANALYZE',
  NULL,
  NULL,
  NULL,
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
)
ON CONFLICT ("id") DO NOTHING;
