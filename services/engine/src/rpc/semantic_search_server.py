import asyncio
import logging
from grpclib.server import Server
from src.ml.semantic_vectorizer import generate_embedding
from src.gen.semantic_search.semantic_search_grpc import SemanticSearchEngineServiceBase
from src.gen.semantic_search.semantic_search_pb import SemanticQueryRequest, SemanticQueryResponse

SemanticQueryRequest.FromString = classmethod(lambda cls, s: cls.from_binary(s))
SemanticQueryResponse.SerializeToString = lambda self: self.to_binary()

logger = logging.getLogger(__name__)

from grpclib.server import Stream

class SemanticSearchEngineService(SemanticSearchEngineServiceBase):
    async def ExecuteSemanticSearchQuery(self, stream: Stream[SemanticQueryRequest, SemanticQueryResponse]):
        logger.info(f"grpc request received: executing semantic search query...")
        
        request = await stream.recv_message()
        text = request.search_query_text
        
        query = f"Represent this sentence for searching relevant passages: {text}"
        
        vector_data = await asyncio.to_thread(generate_embedding, query)
        
        await stream.send_message(SemanticQueryResponse(embedding=vector_data))

async def serve_grpc():
    server = Server([SemanticSearchEngineService()])
    await server.start("0.0.0.0", 50051)
    logger.info("python grpc semantic search engine listening on 0.0.0.0:50051")
    await server.wait_closed()
