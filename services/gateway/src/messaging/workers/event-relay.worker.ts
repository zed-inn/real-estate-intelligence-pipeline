import { db } from "@/db/index";
import { eventRelayQueue, DbEventRelayQueueSchema, DbRealEstateListingSchema } from "@/db/schema";
import { sql, inArray } from "drizzle-orm";
import { producer } from "@/messaging/kafka.client";
import { ListingIngestedEventSchema } from "@/gen/real-estate/listing-events_pb";
import { fromJson, toBinary } from "@bufbuild/protobuf";
import { EVENT_RELAY_BATCH_SIZE } from "@/config/constants";

export function startEventRelayPoller() {
  console.log(
    "starting event relay poller with zod row validation and protobuf serialization...",
  );

  async function poll() {
    try {
      await db.transaction(async (tx: any) => {
        const result = await tx.execute(sql`
          SELECT id, topic, payload, created_at AS "createdAt" FROM event_relay_queue
          ORDER BY created_at ASC
          FOR UPDATE SKIP LOCKED
          LIMIT ${EVENT_RELAY_BATCH_SIZE}
        `);

        if (result.length > 0) {
          const idsToDelete: string[] = [];
          const groupedMessages: Record<string, any[]> = {};

          for (const row of result) {
            const validatedRow = DbEventRelayQueueSchema.parse(row);

            let rawJson;
            try {
              rawJson = JSON.parse(validatedRow.payload);
            } catch (err) {
              console.error(
                `fatal parsing error for event relay queue ${validatedRow.id}`,
                err,
              );
              idsToDelete.push(validatedRow.id);
              continue;
            }

            const dbData = DbRealEstateListingSchema.parse(rawJson);

            const mergedData = {
              listingId: dbData.id,
              listing: {
                ...(dbData.rawFeatures as Record<string, unknown>),
                ...dbData,
              },
            };

            const eventPayload = fromJson(
              ListingIngestedEventSchema,
              mergedData as any,
              { ignoreUnknownFields: true },
            );

            const buffer = toBinary(ListingIngestedEventSchema, eventPayload);

            if (!groupedMessages[validatedRow.topic])
              groupedMessages[validatedRow.topic] = [];
            groupedMessages[validatedRow.topic].push({
              key: eventPayload.listingId,
              value: Buffer.from(buffer),
            });

            idsToDelete.push(validatedRow.id);
          }

          for (const [topic, messages] of Object.entries(groupedMessages)) {
            await producer.send({ topic, messages });
          }

          await tx.delete(eventRelayQueue).where(inArray(eventRelayQueue.id, idsToDelete));
        }
      });
    } catch (error) {
      console.error("event relay poller error:", error);
    } finally {
      setTimeout(poll, 2000);
    }
  }
  poll();
}
