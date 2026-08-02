import { pgTable, uuid, varchar, text, timestamp } from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import { createSelectSchema } from "drizzle-zod";
import { z } from "zod";
import { realEstateListings } from "@/gen/drizzle/real-estate-drizzle-schema";

export * from "@/gen/drizzle/real-estate-drizzle-schema";

export const eventRelayQueue = pgTable("event_relay_queue", {
  id: uuid("id")
    .default(sql`uuidv7()`)
    .primaryKey(),
  topic: varchar("topic", { length: 255 }).notNull(),
  payload: text("payload").notNull(),
  createdAt: timestamp("created_at").defaultNow().notNull(),
});

export const DbRealEstateListingSchema = createSelectSchema(realEstateListings, {
  createdAt: z.coerce.date(),
});

export const DbRealEstateListingInsertSchema = DbRealEstateListingSchema.omit({
  id: true,
  rawFeatures: true,
  createdAt: true,
  embedding: true,
  intelligenceContext: true,
});

export const DbRealEstateListingUpdateEmbeddingSchema = DbRealEstateListingSchema.pick({
  id: true,
  embedding: true,
  intelligenceContext: true,
});

export const DbEventRelayQueueSchema = createSelectSchema(eventRelayQueue, {
  createdAt: z.coerce.date(),
});
