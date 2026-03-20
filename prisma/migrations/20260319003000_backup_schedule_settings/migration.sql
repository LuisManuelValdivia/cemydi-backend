CREATE TABLE "database_backup_schedule" (
  "id" INTEGER NOT NULL DEFAULT 1,
  "isEnabled" BOOLEAN NOT NULL DEFAULT false,
  "intervalDays" INTEGER NOT NULL DEFAULT 1,
  "runAtTime" TEXT NOT NULL DEFAULT '03:00',
  "retentionDays" INTEGER NOT NULL DEFAULT 7,
  "lastRunAt" TIMESTAMP(3),
  "nextRunAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT "database_backup_schedule_pkey" PRIMARY KEY ("id")
);

INSERT INTO "database_backup_schedule" (
  "id",
  "isEnabled",
  "intervalDays",
  "runAtTime",
  "retentionDays"
)
VALUES (1, false, 1, '03:00', 7)
ON CONFLICT ("id") DO NOTHING;
