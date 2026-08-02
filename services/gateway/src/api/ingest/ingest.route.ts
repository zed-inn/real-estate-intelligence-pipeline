import { FastifyPluginAsyncZod } from "fastify-type-provider-zod";
import { ingestListingHandler } from "./ingest.controller";
import { RealEstateListingSchema } from "@/api/schemas/real-estate.schema";

export const ingestRoute: FastifyPluginAsyncZod = async (fastify) => {
  fastify.post(
    "/ingest",
    { schema: { body: RealEstateListingSchema } },
    ingestListingHandler
  );
};
