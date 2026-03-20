CREATE TABLE "user_sessions" (
  "id" TEXT NOT NULL,
  "tokenId" TEXT NOT NULL,
  "userId" INTEGER NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "expiresAt" TIMESTAMP(3) NOT NULL,
  "lastSeenAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "endedAt" TIMESTAMP(3),

  CONSTRAINT "user_sessions_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "login_attempts" (
  "id" SERIAL NOT NULL,
  "userId" INTEGER,
  "correo" TEXT NOT NULL,
  "success" BOOLEAN NOT NULL,
  "reason" TEXT,
  "attemptedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT "login_attempts_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "user_sessions_tokenId_key" ON "user_sessions"("tokenId");
CREATE INDEX "user_sessions_userId_endedAt_expiresAt_idx" ON "user_sessions"("userId", "endedAt", "expiresAt");
CREATE INDEX "user_sessions_lastSeenAt_idx" ON "user_sessions"("lastSeenAt");
CREATE INDEX "login_attempts_attemptedAt_idx" ON "login_attempts"("attemptedAt");
CREATE INDEX "login_attempts_success_attemptedAt_idx" ON "login_attempts"("success", "attemptedAt");

ALTER TABLE "user_sessions"
ADD CONSTRAINT "user_sessions_userId_fkey"
FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "login_attempts"
ADD CONSTRAINT "login_attempts_userId_fkey"
FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE SET NULL ON UPDATE CASCADE;
