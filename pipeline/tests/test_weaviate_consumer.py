from consumers.weavite_consumer import WeaviateConsumer
from dotenv import load_dotenv
import logging
import os

load_dotenv()
logging.basicConfig(levle=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    consumer = WeaviateConsumer(
        bootstrap_servers="kafka:9092",
        topic="veritas-pages",
        group_id="veritas-weaviate-consumer",
        cluster_url=os.environ["WEAVIATE_URL"],
        api_key=os.environ["WEAVIATE_API_KEY"]
    )

    consumer.consume()
    