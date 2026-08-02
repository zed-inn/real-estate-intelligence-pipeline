import { pgTable, pgEnum, uuid, varchar, timestamp, jsonb, boolean, integer, numeric, text } from "drizzle-orm/pg-core";
import { vector } from 'drizzle-orm/pg-core';
import { sql } from 'drizzle-orm';

export const property_typeEnum = pgEnum('property_type', ["APARTMENT", "VILLA", "INDEPENDENT_HOUSE", "BUILDER_FLOOR", "PENTHOUSE", "STUDIO_APARTMENT", "DUPLEX", "TRIPLEX", "FARM_HOUSE", "AGRICULTURAL_LAND", "RESIDENTIAL_PLOT", "COMMERCIAL_OFFICE", "RETAIL_SHOP", "SERVICED_APARTMENT", "CO_WORKING_SPACE"]);


export const realEstateListings = pgTable('real_estate_listings', {
id: uuid('id').default(sql`uuidv7()`).primaryKey(),
city: text('city').notNull(),
state: text('state').notNull(),
pinCode: integer('pin_code').notNull(),
priceCrore: numeric('price_crore', {mode: "number"}).notNull(),
bhk: integer('bhk'),
propertyType: property_typeEnum('property_type'),
rawFeatures: jsonb('raw_features').notNull(),
intelligenceContext: text('intelligence_context'),
embedding: vector('embedding', { dimensions: 384 }),
createdAt: timestamp('created_at').defaultNow().notNull(),
})

