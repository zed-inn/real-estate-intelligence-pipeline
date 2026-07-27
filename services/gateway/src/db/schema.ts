import { pgTable, uuid, varchar, numeric, jsonb, text, timestamp, integer } from 'drizzle-orm/pg-core';
import { vector } from 'drizzle-orm/pg-core';
import { sql } from 'drizzle-orm';

export const properties = pgTable('properties', {
  id: uuid('id').default(sql`uuid_generate_v7()`).primaryKey(),

  city: varchar('city', { length: 100 }).notNull(),
  pinCode: varchar('pin_code', { length: 10 }).notNull(),
  priceCr: numeric('price_cr').notNull(),

  locality: varchar('locality', { length: 255 }),
  societyName: varchar('society_name', { length: 255 }),
  pricePerSqft: numeric('price_per_sqft'),

  bhk: integer('bhk'),
  carpetAreaSqft: integer('carpet_area_sqft'),

  propertyType: varchar('property_type', { length: 50 }),

  rawFeatures: jsonb('raw_features').notNull(),

  intelligenceContext: text('intelligence_context'),
  embedding: vector('embedding', { dimensions: 384 }),
  createdAt: timestamp('created_at').defaultNow().notNull(),
});

export const outbox = pgTable('outbox', {
  id: uuid('id').default(sql`uuid_generate_v7()`).primaryKey(),
  topic: varchar('topic', { length: 255 }).notNull(),
  payload: text('payload').notNull(),
  createdAt: timestamp('created_at').defaultNow().notNull(),
});
