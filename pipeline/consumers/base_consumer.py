
from confluent_kafka import Consumer, KafkaException
import json
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseConsumer:
    def __init__(self, bootstrap_servers, topic, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer = Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest"
        })

    def consume(self):
        self.consumer.subscribe([self.topic])
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    logger.error(f"Consumer Error: {msg.error()}")
                
                value_bytes = msg.value()
                if value_bytes is None:
                    continue
                value_str = value_bytes.decode("utf-8")
    
                data = json.loads(value_str)
                self.process_item(data)
            
        except KeyboardInterrupt:
            logger.info("Consumer interrupted, closing...")
        finally:
            self.consumer.close()

    def process_item(self, item):
        raise NotImplementedError
