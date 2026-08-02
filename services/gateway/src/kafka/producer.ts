import { kafka } from "@/kafka/connection";

export const producer = kafka.producer({
  "enable.idempotence": true,
});

export async function connectKafka() {
  await producer.connect();
  console.log("connected to kafka broker");
}
