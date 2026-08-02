import asyncio
import logging
from grpclib.server import Server
from src.ml.embedder import generate_embedding
from src.gen.vector_grpc import VectorEngineBase
from src.gen.vector_pb import SearchQueryEncodeRequest, EncodeResponse

SearchQueryEncodeRequest.FromString = classmethod(lambda cls, s: cls.from_binary(s))
EncodeResponse.SerializeToString = lambda self: self.to_binary()

logger = logging.getLogger(__name__)

from grpclib.server import Stream

class VectorEngineService(VectorEngineBase):
    async def EncodeSearchQuery(self, stream: Stream[SearchQueryEncodeRequest, EncodeResponse]):
        logger.info(f"grpc request received: embedding search query...")
        
        request = await stream.recv_message()
        text = request.search_query_text
        
        query = f"Represent this sentence for searching relevant passages: {text}"
        
        vector_data = await asyncio.to_thread(generate_embedding, query)
        
        await stream.send_message(EncodeResponse(embedding=vector_data))

async def serve_grpc():
    server = Server([VectorEngineService()])
    await server.start("0.0.0.0", 50051)
    logger.info("python grpc vector engine listening on 0.0.0.0:50051")
    await server.wait_closed()
