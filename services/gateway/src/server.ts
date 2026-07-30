import Fastify from 'fastify';
import { serializerCompiler, validatorCompiler, ZodTypeProvider } from 'fastify-type-provider-zod';
import { env } from '@/env.js';
import { propertyRoutes } from '@/api/routes.js';
import { connectKafka } from '@/kafka/producer.js';
import { startOutboxPoller } from '@/outbox/poller.js';

const fastify = Fastify({ logger: true }).withTypeProvider<ZodTypeProvider>();

fastify.setValidatorCompiler(validatorCompiler);
fastify.setSerializerCompiler(serializerCompiler);

async function start() {
  try {
    await connectKafka();

    fastify.register(propertyRoutes, { prefix: '/api' });

    await fastify.listen({ port: env.PORT, host: env.HOST });

    startOutboxPoller();

  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
}

start();
