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
};
