from confluent_kafka import Consumer, KafkaException
import json
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseConsumer:
    def __init__(self, bootstrap_servers, topic, group_id, retries=10, wait=10):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id

        # Retry Logic for Kafka Connection
        for attempt in range(retries):
            try:
                self.consumer = Consumer({
                    "bootstrap.servers": self.bootstrap_servers,
                    "group.id": self.group_id,
                    "auto.offset.reset": "earliest"
                })
                self.consumer.list_topics(timeout=10)
                logger.info("Connected to Kafka!.")
                break
            except KafkaException:
                logger.warning(f"Kafka not ready, retrying in {wait}s ({attempt + 1}/{retries}).")
                time.sleep(wait)
        else:
            raise RuntimeError("Failed to connect to Kafka after multiple attempts.")

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


if __name__ == "__main__":
    consumer = BaseConsumer(
        bootstrap_servers="kafka:9092",
        topic="veritas-pages",
        group_id="veritas-base-consumer"
    )
    consumer.consume()