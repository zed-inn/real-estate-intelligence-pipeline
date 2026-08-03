import Fastify from "fastify";
import {
  serializerCompiler,
  validatorCompiler,
  ZodTypeProvider,
} from "fastify-type-provider-zod";
import { env } from "@/config/env";
import { ingestRoute } from "@/api/ingest/ingest.route";
import { searchRoute } from "@/api/search/search.route";
import { connectKafka } from "@/messaging/kafka.client";
import { startEmbeddingSyncConsumer } from "@/messaging/consumers/embedding-sync.consumer";
import { startEventRelayPoller } from "@/messaging/workers/event-relay.worker";

const fastify = Fastify({ logger: true }).withTypeProvider<ZodTypeProvider>();

fastify.setValidatorCompiler(validatorCompiler);
fastify.setSerializerCompiler(serializerCompiler);

async function start() {
  try {
    await connectKafka();

    fastify.get("/health", async (request, reply) => {
      return { status: "ok" };
    });

    fastify.register(ingestRoute, { prefix: "/api" });
    fastify.register(searchRoute, { prefix: "/api" });

    await fastify.listen({ port: env.PORT, host: env.HOST });

    startEmbeddingSyncConsumer().catch((err) => {
      fastify.log.error("consumer crashed:", err);
    });

    startEventRelayPoller();
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
}

start();
