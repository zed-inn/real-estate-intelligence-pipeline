import { FastifyPluginAsyncZod } from "fastify-type-provider-zod";
import { searchListingHandler } from "./search.controller";
import { z } from "zod";

export const searchRoute: FastifyPluginAsyncZod = async (fastify) => {
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
    searchListingHandler
  );
};
