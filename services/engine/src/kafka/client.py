from confluent_kafka import Consumer, Producer

def get_kafka_consumer(broker: str, group_id: str) -> Consumer:
    conf = {
        'bootstrap.servers': broker,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False 
    }
    return Consumer(conf)

def get_kafka_producer(broker: str) -> Producer:
    conf = {
        'bootstrap.servers': broker,
        'enable.idempotence': True
    }
    return Producer(conf)
