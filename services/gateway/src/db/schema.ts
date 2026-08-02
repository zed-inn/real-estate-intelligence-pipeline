import { pgTable, uuid, varchar, text, timestamp } from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import { createSelectSchema } from "drizzle-zod";
import { z } from "zod";
import { properties } from "@/gen/drizzle/property_drizzle_schema";

export { properties } from "@/gen/drizzle/property_drizzle_schema";

export const outbox = pgTable("outbox", {
  id: uuid("id")
    .default(sql`uuidv7()`)
    .primaryKey(),
  topic: varchar("topic", { length: 255 }).notNull(),
  payload: text("payload").notNull(),
  createdAt: timestamp("created_at").defaultNow().notNull(),
});

export const DbPropertySchema = createSelectSchema(properties, {
  createdAt: z.coerce.date(),
});
export const DbPropertyInsertSchema = DbPropertySchema.omit({
  id: true,
  rawFeatures: true,
  createdAt: true,
  embedding: true,
  intelligenceContext: true,
});

export const DbPropertyUpdateEmbeddingSchema = DbPropertySchema.pick({
  id: true,
  embedding: true,
  intelligenceContext: true,
});

export const DbOutboxSchema = createSelectSchema(outbox, {
  createdAt: z.coerce.date(),
});
