import logging
from src.config.settings import settings
from src.config.constants import KafkaTopics
from src.messaging.kafka_client import get_kafka_consumer, get_kafka_producer
from src.ml.semantic_vectorizer import generate_embedding
from src.ml.intelligence_context_generator import build_intelligence_context
from src.gen.real_estate.listing_events_pb import ListingIngestedEvent, ListingEmbeddedEvent
import threading

logger = logging.getLogger(__name__)

def run_listing_event_consumer_sync(shutdown_event: threading.Event):
    consumer = get_kafka_consumer(settings.kafka_broker, settings.kafka_group_id)
    producer = get_kafka_producer(settings.kafka_broker)
    
    consumer.subscribe([KafkaTopics.REAL_ESTATE_LISTING_INGESTED])
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
                payload = ListingIngestedEvent.from_binary(msg.value())
                context = build_intelligence_context(payload)
                
                embedding = generate_embedding(context)
                
                embedded_event = ListingEmbeddedEvent(
                    listing_id=payload.listing_id,
                    intelligence_context=context,
                    embedding=embedding
                )
                
                producer.produce(
                    topic=KafkaTopics.REAL_ESTATE_LISTING_EMBEDDED,
                    key=payload.listing_id.encode('utf-8'),
                    value=embedded_event.to_binary()
                )
                
                producer.flush() 
                logger.info(f"successfully published embedding for listing: {payload.listing_id}")
                consumer.commit(message=msg)
                
            except Exception as e:
                logger.error(f"failed to process message: {e}")

    except Exception as e:
        logger.error(f"kafka consumer thread crashed: {e}")
    finally:
        consumer.close()
