import { kafka } from "@/messaging/kafka.client";
import { db } from "@/db/index";
import { realEstateListings, DbRealEstateListingUpdateEmbeddingSchema } from "@/db/schema";
import { eq } from "drizzle-orm";
import { fromBinary } from "@bufbuild/protobuf";
import { env } from "@/config/env";
import { ListingEmbeddedEventSchema } from "@/gen/real-estate/listing-events_pb";
import { KAFKA_TOPICS } from "@/config/constants";

export const consumer = kafka.consumer({
  kafkaJS: {
    groupId: "real-estate-embedding-sync-group",
    fromBeginning: true,
  }
});

export async function startEmbeddingSyncConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPICS.REAL_ESTATE_LISTING_EMBEDDED });
  console.log(`gateway consumer connected and subscribed to ${KAFKA_TOPICS.REAL_ESTATE_LISTING_EMBEDDED}`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      try {
        const rawEvent = fromBinary(ListingEmbeddedEventSchema, message.value);

        const event = DbRealEstateListingUpdateEmbeddingSchema.parse({
          ...rawEvent,
          id: rawEvent.listingId,
        });

        await db
          .update(realEstateListings)
          .set({
            intelligenceContext: event.intelligenceContext,
            embedding: event.embedding,
          })
          .where(eq(realEstateListings.id, event.id));

        console.log(`successfully saved embedding for listing ${event.id}`);
      } catch (err) {
        console.error("failed to process embedded event:", err);
      }
    },
  });
}
