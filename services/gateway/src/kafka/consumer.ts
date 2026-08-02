import { kafka } from "@/kafka/connection";
import { db } from "@/db/index.js";
import { properties, DbPropertyUpdateEmbeddingSchema } from "@/db/schema.js";
import { eq } from "drizzle-orm";
import { fromBinary } from "@bufbuild/protobuf";
import { EmbeddedPropertyEventSchema } from "@/gen/events_pb.js";

export const consumer = kafka.consumer({
  "group.id": "gateway-embedding-updater",
});

export async function startConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: "property.embedded" });
  console.log("gateway consumer connected and subscribed to property.embedded");

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      try {
        const rawEvent = fromBinary(EmbeddedPropertyEventSchema, message.value);

        const event = DbPropertyUpdateEmbeddingSchema.parse({
          ...rawEvent,
          id: rawEvent.propertyId,
        });

        await db
          .update(properties)
          .set({
            intelligenceContext: event.intelligenceContext,
            embedding: event.embedding,
          })
          .where(eq(properties.id, event.id));

        console.log(`successfully saved embedding for property ${event.id}`);
      } catch (err) {
        console.error("failed to process embedded event:", err);
      }
    },
  });
}
