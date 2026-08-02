import logging
import asyncio
import threading
from src.config import settings
from src.kafka.client import get_kafka_consumer, get_kafka_producer
from src.ml.embedder import generate_embedding
from src.ml.context_builder import build_intelligence_context
from src.gen.events_pb import PropertyIngestedEvent, EmbeddedPropertyEvent
from src.rpc.server import serve_grpc

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

shutdown_event = threading.Event()

def run_kafka_consumer_sync():
    consumer = get_kafka_consumer(settings.kafka_broker, settings.kafka_group_id)
    producer = get_kafka_producer(settings.kafka_broker)
    
    consumer.subscribe(['property.ingested'])
    logger.info("kafka ingestion consumer started (running in dedicated thread)")

    try:
        while not shutdown_event.is_set():
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                logger.error(f"kafka error: {msg.error()}")
                continue
            
            try:
                payload = PropertyIngestedEvent.from_binary(msg.value())
                context = build_intelligence_context(payload)
                
                embedding = generate_embedding(context)
                
                embedded_event = EmbeddedPropertyEvent(
                    property_id=payload.property_id,
                    intelligence_context=context,
                    embedding=embedding
                )
                
                producer.produce(
                    topic='property.embedded',
                    key=payload.property_id.encode('utf-8'),
                    value=embedded_event.to_binary()
                )
                
                producer.flush() 
                logger.info(f"successfully published embedding for property: {payload.property_id}")
                consumer.commit(message=msg)
                
            except Exception as e:
                logger.error(f"failed to process message: {e}")

    except Exception as e:
        logger.error(f"kafka consumer thread crashed: {e}")
    finally:
        consumer.close()

async def main():
    logger.info("starting python intelligence engine...")
    
    try:
        await asyncio.gather(
            asyncio.to_thread(run_kafka_consumer_sync),
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
