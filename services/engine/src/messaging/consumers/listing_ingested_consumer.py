import logging
import threading
from src.config.settings import settings
from src.config.constants import KafkaTopics
from src.messaging.kafka_client import get_kafka_consumer, get_kafka_producer
from src.ml.semantic_vectorizer import generate_embedding
from src.ml.intelligence_context_generator import build_intelligence_context
from src.gen.real_estate.listing_events_pb import ListingIngestedEvent, ListingEmbeddedEvent
from prometheus_client import Summary, Counter

logger = logging.getLogger(__name__)

EMBEDDING_TIME = Summary('engine_embedding_processing_seconds', 'Time spent generating embeddings')
EMBEDDING_COUNT = Counter('engine_embeddings_total', 'Total number of embeddings generated')

@EMBEDDING_TIME.time()
def process_ingested_message(msg_value: bytes, producer) -> str:
    payload = ListingIngestedEvent.from_binary(msg_value)
    context = build_intelligence_context(payload)
    
    embedding = generate_embedding(context)
    
    embedded_event = ListingEmbeddedEvent(
            listing_id=payload.listing_id,
            intelligence_context=context,
            embedding=embedding
    )
    
    EMBEDDING_COUNT.inc()
    
    producer.produce(
        topic=KafkaTopics.REAL_ESTATE_LISTING_EMBEDDED,
        key=payload.listing_id.encode('utf-8'),
        value=embedded_event.to_binary()
    )
    producer.flush() 
    return payload.listing_id

def route_to_dlq(msg, producer, err_msg: str):
    try:
        producer.produce(
            topic=KafkaTopics.REAL_ESTATE_LISTING_DLQ,
            key=msg.key(),
            value=msg.value(),
            headers=[
                ('error', err_msg.encode('utf-8')),
                ('original_topic', msg.topic().encode('utf-8')),
                ('original_partition', str(msg.partition()).encode('utf-8'))
            ]
        )
        producer.flush()
    except Exception as dlq_err:
        logger.critical(f"critical: failed to route message to dlq: {dlq_err}")

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
                listing_id = process_ingested_message(msg.value(), producer)
                logger.info(f"successfully published embedding for listing: {listing_id}")
            except Exception as e:
                logger.error(f"failed to process message, routing directly to dlq: {e}")
                route_to_dlq(msg, producer, str(e))
            
            # unconditionally commit the message offset regardless of whether it succeeded or was sent to dlq
            consumer.commit(message=msg)

    except Exception as e:
        logger.error(f"kafka consumer thread crashed: {e}")
    finally:
        consumer.close()
