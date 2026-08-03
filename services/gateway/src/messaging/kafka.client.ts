import { KafkaJS } from "@confluentinc/kafka-javascript";
import { env } from "@/config/env";

const { Kafka } = KafkaJS;

export const kafka = new Kafka({
  "bootstrap.servers": env.KAFKA_BROKER,
  "client.id": env.KAFKA_CLIENT_ID,
});

export const producer = kafka.producer({
  "enable.idempotence": true,
});

export async function connectKafka() {
  let retries = 10;
  while (retries > 0) {
    try {
      await producer.connect();
      console.log("connected to kafka broker");
      return;
    } catch (err) {
      console.error(`Kafka connection failed, retrying... (${retries} attempts left)`);
      retries--;
      await new Promise(resolve => setTimeout(resolve, 3000));
    }
  }
  throw new Error("Failed to connect to Kafka after multiple retries.");
}
