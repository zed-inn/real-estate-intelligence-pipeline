import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { env } from "@/config/env.js";
import { VectorEngine } from "@/gen/vector_pb.js";

const transport = createGrpcTransport({
  baseUrl: env.PYTHON_ENGINE_GRPC_URL,
});

const client = createClient(VectorEngine, transport);

export const encodeSearchQuery = async (query: string): Promise<number[]> => {
  const response = await client.encodeSearchQuery({ searchQueryText: query });
  return response.embedding;
};
