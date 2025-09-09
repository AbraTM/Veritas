import weaviate
import weaviate.classes as wvc
import weaviate.auth as wv_auth
import logging
import os
from consumers.base_consumer import BaseConsumer
from processors.embeddings import get_embedding_model
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeaviateConsumer(BaseConsumer):
    def __init__(self, bootstrap_servers, topic, group_id, cluster_url, api_key, class_name="Page"):
        super().__init__(bootstrap_servers, topic, group_id)
        self.cluster_url = cluster_url
        self.api_key = api_key
        self.class_name = class_name
        self.client = None
        self.embedding_model = get_embedding_model()
    
    def connect_vector_db(self):
        if not self.cluster_url or not self.api_key:
            logger.error("Missing Weaviate cluster URL or API key.")
            raise ValueError("Missing Weaviate cluster URL or API key")

        try:
            # Connect to Weaviate Cloud
            self.client = weaviate.connect_to_weaviate_cloud(
                cluster_url=self.cluster_url,
                auth_credentials=wv_auth.AuthApiKey(self.api_key)
            )

            if not self.client.is_ready():
                logger.error("Failed to connect to Weaviate Cloud.")
                raise ConnectionError("Failed to connect to Weaviate Cloud")
            
            logger.info(f"Connected to Weaviate Cloud at {self.cluster_url}.")

            if not self.client.collections.exists(self.class_name):
                logger.info(f"Collection '{self.class_name}' not found. Creating...")

                self.client.collections.create(
                    name=self.class_name,
                    vectorizer_config=wvc.config.Configure.Vectorizer.none(),
                    properties=[
                        wvc.config.Property(name="title", data_type=wvc.config.DataType.TEXT),
                        wvc.config.Property(name="abstract", data_type=wvc.config.DataType.TEXT),
                        wvc.config.Property(name="published", data_type=wvc.config.DataType.DATE),
                        wvc.config.Property(name="updated", data_type=wvc.config.DataType.DATE),
                        wvc.config.Property(name="authors", data_type=wvc.config.DataType.TEXT_ARRAY),
                        wvc.config.Property(name="link", data_type=wvc.config.DataType.TEXT),
                        wvc.config.Property(name="source_category", data_type=wvc.config.DataType.TEXT),
                    ]
                )
                logger.info(f"Collection '{self.class_name}' created successfully.")
            else:
                logger.info(f"Collection '{self.class_name}' already exists.")
        except Exception as e:
            logger.error(f"Error connecting to Weaviate: {e}")
            if self.client:
                self.client.close()
            raise
    
    def disconnect_vector_db(self):
        if self.client:
            self.client.close()
            logger.info("Disconnected from Weaviate.")
    
    def process_item(self, item):
        if not self.client or not self.client.is_ready():
            logger.error("Weaviate client is not ready. Cannot process item.")
            return
            
        try:
            collection = self.client.collections.get(self.class_name)
            
            abstract_text = item.get("abstract") or ""
            abstract_vector = self.embedding_model.encode(abstract_text, normalize_embeddings=True)
            
            collection.data.insert(
                properties={
                    "title": item.get("title"),
                    "abstract": item.get("abstract"),
                    "link": item.get("link"),
                    "source_category": item.get("source_category"),
                    "published": item.get("published"),
                    "updated": item.get("updated"),
                    "authors": item.get("authors", [])
                },
                vector=abstract_vector.tolist()
            )
            logger.info(f"Added item {item.get('id')} from {item.get('source_category')} to Weaviate.")
        except Exception as e:
            logger.error(f"Failed to add item {item.get('id')} from {item.get('source_category')} to Weaviate: {e}")

    
    def consume(self):
        self.connect_vector_db()
        try:
            super().consume()
        finally:
            self.disconnect_vector_db()


# To run the consumer
if __name__ == "__main__":
    consumer = WeaviateConsumer(
        bootstrap_servers="kafka:9092",
        topic="veritas-pages",
        group_id="veritas-weaviate-consumer",
        cluster_url=os.environ["WEAVIATE_URL"],
        api_key=os.environ["WEAVIATE_API_KEY"]
    )

    consumer.consume()