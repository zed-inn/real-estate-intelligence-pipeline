import { FastifyRequest, FastifyReply } from "fastify";
import { cosineDistance, sql } from "drizzle-orm";
import { executeSemanticSearchQuery } from "./semantic-search.client";
import { db } from "@/db/index";
import { realEstateListings } from "@/db/schema";

type SearchQuery = {
  q: string;
  limit: number;
  page: number;
};

export const searchListingHandler = async (request: FastifyRequest, reply: FastifyReply) => {
  const { q, limit, page } = request.query as SearchQuery;
  const offset = (page - 1) * limit;

  try {
    const queryVector = await executeSemanticSearchQuery(q);

    const distance = cosineDistance(realEstateListings.embedding, queryVector);

    const results = await db
      .select({
        id: realEstateListings.id,
        city: realEstateListings.city,
        priceCrore: realEstateListings.priceCrore,
        bhk: realEstateListings.bhk,
        propertyType: realEstateListings.propertyType,
        intelligenceContext: realEstateListings.intelligenceContext,
        similarityScore: sql<number>`1 - (${distance})`.as("similarityScore"),
      })
      .from(realEstateListings)
      .where(sql`${realEstateListings.embedding} IS NOT NULL AND ${distance} < 0.5`)
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
};
