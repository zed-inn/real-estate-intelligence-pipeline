import { FastifyPluginAsyncZod } from "fastify-type-provider-zod";
import {
  PropertySchema,
  PropertySearchableSchema,
} from "@/api/schemas/property";
import {
  DbPropertySchema,
  DbPropertyInsertSchema,
  outbox,
  properties,
} from "@/db/schema";
import { db } from "@/db";
import { KAFKA_TOPICS } from "@/config/constants";
import { cosineDistance, sql, isNotNull } from "drizzle-orm";
import { encodeSearchQuery } from "@/grpc/client";
import { z } from "zod";

export const propertyRoutes: FastifyPluginAsyncZod = async (fastify) => {
  fastify.post(
    "/ingest",
    { schema: { body: PropertySchema } },
    async (request, reply) => {
      const body = request.body;

      const searchableFields = PropertySearchableSchema.parse(body);
      const rawFeatures: Record<string, unknown> = {};

      for (const [key, value] of Object.entries(body))
        if (!Object.hasOwn(searchableFields, key)) rawFeatures[key] = value;

      const generatedId = await db.transaction(async (tx) => {
        const dbInsertPayload = DbPropertyInsertSchema.parse(searchableFields);

        const [newProp] = await tx
          .insert(properties)
          .values({
            ...dbInsertPayload,
            rawFeatures,
          })
          .returning();

        const propInserted = DbPropertySchema.parse(newProp);

        await tx.insert(outbox).values({
          topic: KAFKA_TOPICS.PROPERTY_INGESTED,
          payload: JSON.stringify(propInserted),
        });

        return propInserted.id;
      });

      reply.statusCode = 201;
      return { success: true, property_id: generatedId };
    },
  );

  fastify.get(
    "/search",
    {
      schema: {
        querystring: z.object({
          q: z.string().min(2, "Search query is too short"),
          limit: z.coerce.number().min(1).max(50).default(10),
          page: z.coerce.number().min(1).default(1),
        }),
      },
    },
    async (request, reply) => {
      const { q, limit, page } = request.query;
      const offset = (page - 1) * limit;

      try {
        const queryVector = await encodeSearchQuery(q);

        const distance = cosineDistance(properties.embedding, queryVector);

        const results = await db
          .select({
            id: properties.id,
            city: properties.city,
            priceCrore: properties.priceCrore,
            bhk: properties.bhk,
            propertyType: properties.propertyType,
            intelligenceContext: properties.intelligenceContext,
            similarityScore: sql<number>`1 - ${distance}`.as("similarityScore"),
          })
          .from(properties)
          .where(isNotNull(properties.embedding))
          .orderBy(distance)
          .limit(limit)
          .offset(offset);

        return {
          success: true,
          query: q,
          page,
          limit,
          results,
        };
      } catch (err) {
        request.log.error(err, "search failed:");
        reply.statusCode = 500;
        return { success: false, error: "Semantic search engine unavailable" };
      }
    },
  );
};
