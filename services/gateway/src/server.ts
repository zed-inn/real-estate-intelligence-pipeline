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
import fastifyRateLimit from "@fastify/rate-limit";

const fastify = Fastify({
  logger: true,
  bodyLimit: 1048576,
}).withTypeProvider<ZodTypeProvider>();

fastify.setValidatorCompiler(validatorCompiler);
fastify.setSerializerCompiler(serializerCompiler);

async function start() {
  try {
    await connectKafka();

    await fastify.register(fastifyRateLimit, {
      max: 100,
      timeWindow: "1 minute",
    });

    fastify.get("/health", async (request, reply) => {
      return { status: "ok" };
    });

    fastify.register(ingestRoute, { prefix: "/api" });
    fastify.register(searchRoute, { prefix: "/api" });

    await fastify.listen({ port: env.PORT, host: env.HOST });

    startEmbeddingSyncConsumer().catch((err) => {
      fastify.log.error("consumer crashed:", err);
    });

    const abortController = new AbortController();
    startEventRelayPoller(abortController.signal);

    const shutdown = async () => {
      fastify.log.info("shutting down gracefully...");
      abortController.abort();
      await fastify.close();
      process.exit(0);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
}

start();
