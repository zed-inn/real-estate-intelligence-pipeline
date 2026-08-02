import { db } from "@/db/index.js";
import { outbox, DbOutboxSchema, DbPropertySchema } from "@/db/schema";
import { sql, inArray } from "drizzle-orm";
import { producer } from "@/kafka/producer.js";
import { PropertyIngestedEventSchema } from "@/gen/events_pb.js";
import { fromJson, toBinary } from "@bufbuild/protobuf";
import { OUTBOX_BATCH_SIZE } from "@/config/constants.js";

export function startOutboxPoller() {
  console.log(
    "starting outbox poller with zod row validation and protobuf serialization...",
  );

  async function poll() {
    try {
      await db.transaction(async (tx) => {
        const result = await tx.execute(sql`
          SELECT id, topic, payload, created_at AS "createdAt" FROM outbox
          ORDER BY created_at ASC
          FOR UPDATE SKIP LOCKED
          LIMIT ${OUTBOX_BATCH_SIZE}
        `);

        if (result.length > 0) {
          const idsToDelete: string[] = [];
          const groupedMessages: Record<string, any[]> = {};

          for (const row of result) {
            const validatedRow = DbOutboxSchema.parse(row);

            let rawJson;
            try {
              rawJson = JSON.parse(validatedRow.payload);
            } catch (err) {
              console.error(
                `fatal parsing error for outbox ${validatedRow.id}`,
                err,
              );
              idsToDelete.push(validatedRow.id);
              continue;
            }

            const dbData = DbPropertySchema.parse(rawJson);

            const mergedData = {
              propertyId: dbData.id,
              property: {
                ...(dbData.rawFeatures as Record<string, unknown>),
                ...dbData,
              },
            };

            const eventPayload = fromJson(
              PropertyIngestedEventSchema,
              mergedData as any,
              { ignoreUnknownFields: true },
            );

            const buffer = toBinary(PropertyIngestedEventSchema, eventPayload);

            if (!groupedMessages[validatedRow.topic])
              groupedMessages[validatedRow.topic] = [];
            groupedMessages[validatedRow.topic].push({
              key: eventPayload.propertyId,
              value: Buffer.from(buffer),
            });

            idsToDelete.push(validatedRow.id);
          }

          for (const [topic, messages] of Object.entries(groupedMessages)) {
            await producer.send({ topic, messages });
          }

          await tx.delete(outbox).where(inArray(outbox.id, idsToDelete));
        }
      });
    } catch (error) {
      console.error("outbox poller error:", error);
    } finally {
      setTimeout(poll, 2000);
    }
  }
  poll();
}
