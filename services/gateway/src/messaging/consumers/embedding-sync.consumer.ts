import { kafka, producer } from "@/messaging/kafka.client";
import { db } from "@/db/index";
import { realEstateListings, DbRealEstateListingUpdateEmbeddingSchema } from "@/db/schema";
import { eq } from "drizzle-orm";
import { fromBinary } from "@bufbuild/protobuf";
import { ListingEmbeddedEventSchema } from "@/gen/real-estate/listing-events_pb";
import { KAFKA_TOPICS } from "@/config/constants";

export const consumer = kafka.consumer({
  "allow.auto.create.topics": true,
  kafkaJS: {
    groupId: "real-estate-embedding-sync-group",
    fromBeginning: true,
  }
});

async function processEmbeddedMessage(value: Buffer) {
  const rawEvent = fromBinary(ListingEmbeddedEventSchema, value);
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
}

async function routeToDLQ(message: any, topic: string, partition: number, err: any) {
  try {
    await producer.send({
      topic: KAFKA_TOPICS.REAL_ESTATE_LISTING_DLQ,
      messages: [{
        key: message.key,
        value: message.value,
        headers: {
          error: Buffer.from(err.message || "unknown error"),
          original_topic: Buffer.from(topic),
          original_partition: Buffer.from(partition.toString())
        }
      }]
    });
  } catch (dlqErr: any) {
    console.error(`critical: failed to route message to dlq:`, dlqErr.message);
  }
}

export async function startEmbeddingSyncConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPICS.REAL_ESTATE_LISTING_EMBEDDED });
  console.log(`gateway consumer connected and subscribed to ${KAFKA_TOPICS.REAL_ESTATE_LISTING_EMBEDDED}`);

  await consumer.run({
    eachMessage: async ({ message, topic, partition }) => {
      if (!message.value) return;

      try {
        await processEmbeddedMessage(message.value);
      } catch (err: any) {
        console.error(`failed to process embedded event, routing directly to dlq:`, err.message);
        await routeToDLQ(message, topic, partition, err);
      }
    },
  });
}
