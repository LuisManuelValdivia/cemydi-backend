CREATE TABLE "catalog"."product_images" (
    "id" SERIAL NOT NULL,
    "productId" INTEGER NOT NULL,
    "imageUrl" TEXT NOT NULL,
    "cloudinaryPublicId" TEXT,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "product_images_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "product_images_productId_sortOrder_idx"
ON "catalog"."product_images"("productId", "sortOrder");

ALTER TABLE "catalog"."product_images"
ADD CONSTRAINT "product_images_productId_fkey"
FOREIGN KEY ("productId")
REFERENCES "catalog"."products"("id")
ON DELETE CASCADE
ON UPDATE CASCADE;
