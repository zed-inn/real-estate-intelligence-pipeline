import { z } from "zod";
import dotenv from "dotenv";

dotenv.config();

const envSchema = z.object({
  NODE_ENV: z.enum(["development", "production", "test"]).default("development"),
  HOST: z.string().min(1).default("0.0.0.0"),
  PORT: z.coerce.number().default(3000),
  
  DATABASE_URL: z.string().url(),
  
  KAFKA_BROKER: z.string().min(1).default("localhost:9092"),
  KAFKA_CLIENT_ID: z.string().min(1).default("gateway-producer"),
});

const _env = envSchema.safeParse(process.env);

if (!_env.success) {
  console.error("invalid environment variables:", z.treeifyError(_env.error));
  process.exit(1);
}

export const env = _env.data;
