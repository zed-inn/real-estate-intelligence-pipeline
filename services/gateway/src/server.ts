import Fastify from "fastify";
import {
  serializerCompiler,
  validatorCompiler,
  ZodTypeProvider,
} from "fastify-type-provider-zod";
import { env } from "@/config/env.js";
import { propertyRoutes } from "@/api/routes";
import { connectKafka } from "@/kafka/producer";
import { startConsumer } from "@/kafka/consumer";
import { startOutboxPoller } from "@/outbox/poller";

const fastify = Fastify({ logger: true }).withTypeProvider<ZodTypeProvider>();

fastify.setValidatorCompiler(validatorCompiler);
fastify.setSerializerCompiler(serializerCompiler);

async function start() {
  try {
    await connectKafka();

    fastify.register(propertyRoutes, { prefix: "/api" });

    await fastify.listen({ port: env.PORT, host: env.HOST });

    startConsumer().catch((err) => {
      fastify.log.error("consumer crashed:", err);
    });

    startOutboxPoller();
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
}

start();
