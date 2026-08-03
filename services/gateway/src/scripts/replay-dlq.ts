import { kafka } from "@/messaging/kafka.client";
import { KAFKA_TOPICS } from "@/config/constants";

async function replayDLQ() {
  const consumer = kafka.consumer({
    "enable.auto.commit": false,
    kafkaJS: {
      groupId: "dlq-replay-group",
      fromBeginning: true,
    },
  });
  const producer = kafka.producer();

  await consumer.connect();
  await producer.connect();

  console.log("Connected to Kafka. Starting DLQ replay...");

  await consumer.subscribe({ topic: KAFKA_TOPICS.REAL_ESTATE_LISTING_DLQ });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const originalTopic = message.headers?.original_topic?.toString();
        const errorReason = message.headers?.error?.toString();

        if (!originalTopic) {
          console.error(
            `Message offset ${message.offset} has no original_topic header. Skipping.`,
          );
          return;
        }

        console.log(
          `Replaying message offset ${message.offset} to ${originalTopic}. Original error: ${errorReason}`,
        );

        await producer.send({
          topic: originalTopic,
          messages: [
            {
              key: message.key,
              value: message.value,
              headers: {
                replayed: "true",
                replayed_at: new Date().toISOString(),
              },
            },
          ],
        });

        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (BigInt(message.offset) + BigInt(1)).toString(),
          },
        ]);
      } catch (err) {
        console.error(
          `Failed to replay message offset ${message.offset}:`,
          err,
        );
        process.exit(1);
      }
    },
  });

  console.log("Listening for messages... Press Ctrl+C to exit.");

  process.on("SIGINT", async () => {
    console.log("Shutting down DLQ replay...");
    await consumer.disconnect();
    await producer.disconnect();
    process.exit(0);
  });
}

replayDLQ().catch(console.error);
