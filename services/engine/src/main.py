import logging
from src.config import settings
from src.kafka.client import get_kafka_consumer, get_kafka_producer
from src.ml.embedder import build_intelligence_context, generate_embedding
from generated.intelligence_events import PropertyIngestedEvent, EmbeddedPropertyEvent

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    consumer = get_kafka_consumer(settings.kafka_broker, settings.kafka_group_id)
    producer = get_kafka_producer(settings.kafka_broker)
    
    consumer.subscribe(['property.ingested'])
    logger.info("python compute engine started")

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                logger.error(f"kafka error: {msg.error()}")
                continue
            
            try:
                payload = PropertyIngestedEvent().parse(msg.value())
                
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
                    value=bytes(embedded_event)
                )
                
                producer.flush() 
                logger.info(f"successfully published embedding for property: {payload.property_id}")
                
                consumer.commit(message=msg)
                
            except Exception as e:
                logger.error(f"failed to process message: {e}")

    except KeyboardInterrupt:
        logger.info("shutting down engine...")
        exit()
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
