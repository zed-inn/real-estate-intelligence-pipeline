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
  await producer.connect();
  console.log("connected to kafka broker");
}
