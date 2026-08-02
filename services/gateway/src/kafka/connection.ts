import { KafkaJS } from "@confluentinc/kafka-javascript";
import { env } from "@/config/env.js";

const { Kafka } = KafkaJS;

export const kafka = new Kafka({
  "bootstrap.servers": env.KAFKA_BROKER,
  "client.id": env.KAFKA_CLIENT_ID,
});
