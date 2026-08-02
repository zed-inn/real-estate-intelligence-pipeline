import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { env } from "@/config/env";
import { SemanticSearchEngineService } from "@/gen/semantic-search/semantic-search_pb";

const transport = createGrpcTransport({
  baseUrl: env.PYTHON_ENGINE_GRPC_URL,
});

const client = createClient(SemanticSearchEngineService, transport);

export const executeSemanticSearchQuery = async (query: string): Promise<number[]> => {
  const response = await client.executeSemanticSearchQuery({ searchQueryText: query });
  return response.embedding;
};
