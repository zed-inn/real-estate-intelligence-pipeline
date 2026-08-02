import { FastifyRequest, FastifyReply } from "fastify";
import { RealEstateListingSearchableSchema } from "@/api/schemas/real-estate.schema";
import {
  DbRealEstateListingSchema,
  DbRealEstateListingInsertSchema,
  eventRelayQueue,
  realEstateListings,
} from "@/db/schema";
import { db } from "@/db/index";
import { KAFKA_TOPICS } from "@/config/constants";

export const ingestListingHandler = async (request: FastifyRequest, reply: FastifyReply) => {
  const body = request.body as Record<string, unknown>;

  const searchableFields = RealEstateListingSearchableSchema.parse(body);
  const rawFeatures: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(body))
    if (!Object.hasOwn(searchableFields, key)) rawFeatures[key] = value;

  const generatedId = await db.transaction(async (tx) => {
    const dbInsertPayload = DbRealEstateListingInsertSchema.parse(searchableFields);

    const [newListing] = await tx
      .insert(realEstateListings)
      .values({
        ...dbInsertPayload,
        rawFeatures,
      })
      .returning();

    const insertedListing = DbRealEstateListingSchema.parse(newListing);

    await tx.insert(eventRelayQueue).values({
      topic: KAFKA_TOPICS.REAL_ESTATE_LISTING_INGESTED,
      payload: JSON.stringify(insertedListing),
    });

    return insertedListing.id;
  });

  reply.statusCode = 201;
  return { success: true, listing_id: generatedId };
};
