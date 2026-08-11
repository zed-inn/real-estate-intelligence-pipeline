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

// rrf constant
const RRF_K = 60;

export const searchListingHandler = async (request: FastifyRequest, reply: FastifyReply) => {
  const { q, limit, page } = request.query as SearchQuery;
  const offset = (page - 1) * limit;

  try {
    const queryVector = await executeSemanticSearchQuery(q);
    const distance = cosineDistance(realEstateListings.embedding, queryVector);
    
    // dynamically adjust pool size to ensure pagination works, maintaining a deep overlap pool
    const fetchLimit = Math.max(60, offset + limit + 20);

    const [vectorResults, keywordResults] = await Promise.all([
      db.select({
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
        .limit(fetchLimit),

      db.select({
          id: realEstateListings.id,
          city: realEstateListings.city,
          priceCrore: realEstateListings.priceCrore,
          bhk: realEstateListings.bhk,
          propertyType: realEstateListings.propertyType,
          intelligenceContext: realEstateListings.intelligenceContext,
          ftsScore: sql<number>`ts_rank_cd(to_tsvector('english', ${realEstateListings.intelligenceContext}), plainto_tsquery('english', ${q}))`.as("ftsScore"),
        })
        .from(realEstateListings)
        .where(sql`to_tsvector('english', ${realEstateListings.intelligenceContext}) @@ plainto_tsquery('english', ${q})`)
        .orderBy(sql`ts_rank_cd(to_tsvector('english', ${realEstateListings.intelligenceContext}), plainto_tsquery('english', ${q})) DESC`)
        .limit(fetchLimit)
    ]);

    const fusedResults = new Map<string, any>();

    // vector results fusion
    vectorResults.forEach((doc, index) => {
      const rank = index + 1;
      fusedResults.set(doc.id, {
        ...doc,
        rrfScore: 1.0 / (RRF_K + rank)
      });
    });

    // keyword results fusion
    keywordResults.forEach((doc, index) => {
      const rank = index + 1;
      const existing = fusedResults.get(doc.id);
      if (existing) {
        existing.rrfScore += 1.0 / (RRF_K + rank);
        existing.ftsScore = doc.ftsScore;
      } else {
        fusedResults.set(doc.id, {
          ...doc,
          rrfScore: 1.0 / (RRF_K + rank)
        });
      }
    });

    const sortedResults = Array.from(fusedResults.values())
      .sort((a, b) => b.rrfScore - a.rrfScore)
      .slice(offset, offset + limit);

    return {
      success: true,
      query: q,
      page,
      limit,
      results: sortedResults,
    };
  } catch (err) {
    request.log.error(err, "hybrid search failed:");
    reply.statusCode = 500;
    return { success: false, error: "hybrid search engine unavailable" };
  }
};
