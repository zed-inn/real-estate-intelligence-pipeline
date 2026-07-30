import { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import { IngestPropertySchema } from '@/schemas/api.js';
import { db } from '@/db/index.js';
import { properties, outbox } from '@/db/schema.js';
import { KAFKA_TOPICS } from '@/constants/index.js';

export const propertyRoutes: FastifyPluginAsyncZod = async (fastify) => {

  fastify.post(
    '/ingest',
    { schema: { body: IngestPropertySchema } },
    async (request, reply) => {
      const data = request.body;

      const {
        city, pin_code, price_cr,
        locality, society_name, price_per_sqft,
        bhk, carpet_area_sqft, property_type,
        ...sparseFeatures
      } = data;

      let generatedPropertyId: string = '';

      await db.transaction(async (tx) => {
        const [newProp] = await tx.insert(properties).values({
          city,
          locality: locality ?? null,
          societyName: society_name ?? null,
          pinCode: pin_code,
          priceCr: price_cr.toString(),
          pricePerSqft: price_per_sqft?.toString() ?? null,
          bhk: bhk ?? null,
          carpetAreaSqft: carpet_area_sqft ?? null,
          propertyType: property_type?.toString() ?? null,

          rawFeatures: sparseFeatures
        }).returning();

        generatedPropertyId = newProp.id;

        await tx.insert(outbox).values({
          topic: KAFKA_TOPICS.PROPERTY_INGESTED,
          payload: JSON.stringify(newProp),
        });
      });

      return reply.status(201).send({
        success: true,
        message: 'property ingested',
        property_id: generatedPropertyId
      });
    }
  );
};
