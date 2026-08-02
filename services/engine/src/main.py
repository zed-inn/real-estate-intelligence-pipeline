import logging
import asyncio
import threading
from src.messaging.consumers.listing_ingested_consumer import run_listing_event_consumer_sync
from src.rpc.semantic_search_server import serve_grpc

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

shutdown_event = threading.Event()

async def main():
    logger.info("starting python intelligence engine...")
    
    try:
        await asyncio.gather(
            asyncio.to_thread(run_listing_event_consumer_sync, shutdown_event),
            serve_grpc(),
        )
    except asyncio.CancelledError:
        logger.info("received cancellation signal. safely shutting down background threads...")
        shutdown_event.set()
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("received termination signal, shutting down engine...")
        shutdown_event.set()
        logger.info("engine terminated")
