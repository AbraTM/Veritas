from consumers.base_consumer import BaseConsumer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestConsumer(BaseConsumer):
    def process_item(self, item):
        logger.info(item)

if __name__ == "__main__":
    consumer = TestConsumer(
        bootstrap_servers="localhost:9092",
        topic="veritas-pages",
        group_id="veritas-base-consumer"
    )
    consumer.consume()