import { db } from '@/db/index.js';
import { outbox, PropertySchema } from '@/db/schema.js';
import { sql, inArray } from 'drizzle-orm';
import { producer } from '@/kafka/producer.js';
import { PropertyIngestedEvent } from '@/generated/events.js';
import { OUTBOX_BATCH_SIZE } from '@/constants/index.js';

export function startOutboxPoller() {
  console.log('starting transactional outbox poller with strict zod validation & protobuf serialization...');

  async function poll() {
    try {
      await db.transaction(async (tx) => {
        // cte with row level lock
        const result = await tx.execute(sql`
          SELECT id, topic, payload FROM outbox
          ORDER BY created_at ASC
          FOR UPDATE SKIP LOCKED
          LIMIT ${OUTBOX_BATCH_SIZE}
        `);

        if (result.length > 0) {
          const idsToDelete: string[] = [];
          const groupedMessages: Record<string, any[]> = {};

          for (const row of result) {
            const outboxId = row.id as string;
            const topic = row.topic as string;
            const payloadStr = row.payload as string;

            const rawJson = JSON.parse(payloadStr);

            const dbData = PropertySchema.parse(rawJson);

            const mergedData = {
              ...(dbData.rawFeatures as Record<string, any>),
              ...dbData,
              propertyId: dbData.id,
            };

            const eventPayload = PropertyIngestedEvent.fromJSON(mergedData);

            const buffer = PropertyIngestedEvent.encode(eventPayload).finish();

            if (!groupedMessages[topic]) groupedMessages[topic] = [];

            groupedMessages[topic].push({
              key: dbData.id,
              value: Buffer.from(buffer),
            });

            idsToDelete.push(outboxId);
          }

          // batch kafka publish
          for (const [topic, messages] of Object.entries(groupedMessages)) {
            await producer.send({ topic, messages });
          }

          if (idsToDelete.length > 0) {
            await tx.delete(outbox).where(inArray(outbox.id, idsToDelete));
          }
        }
      });
    } catch (error) {
      console.error('outbox poller error:', error);
    } finally {
      setTimeout(poll, 2000);
    }
  }

  poll();
}
