import logging
import os
import psycopg2
from consumers.base_consumer import BaseConsumer
from dotenv import load_dotenv

load_dotenv()
POSTGRES_HOST=os.environ["POSTGRES_HOS"]
POSTGRES_USER=os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD=os.environ["POSTGRES_PASSWORD"]
POSTGRES_PORT=os.environ["POSTGRES_PORT"]
POSTGRES_DATABASE=os.environ["POSTGRES_DATABASE"]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgresConsumer(BaseConsumer):
    def __init__(self, bootstrap_servers, topic, group_id, db_config):
        super().__init__(bootstrap_servers, topic, group_id)
        self.db_config = db_config
        self.conn = None
        self.cursor = None

    def open_db(self):
        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()
        logger.info("Connected to PostgreSQL Database.")
    
    def close_db(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def process_item(self, item):
        try:
            self.cursor.execute(
                """
                    INSERT INTO pages (title, abstract, published, updated, authors, link, source_category)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)  
                    ON CONFLICT (link) DO NOTHING      
                """
            , (
                item.get("title"), 
                item.get("abstract"), 
                item.get("published"), 
                item.get("updated"), 
                item.get("authors"), 
                item.get("link"), 
                item.get("source_category")
            ))

            self.conn.commit()
            logger.info(f"Inserted Item {item.get('id')} from {item.get('source_category')}")
        except Exception as e:
            logger.info(f"Failed to insert item {item.get('id')} from {item.get('source_category')}, \n {e}")
            self.conn.rollback()
        
    def consume(self):
        self.open_db()
        try:
            super().consume()
        finally:
            self.close_db()

# To run the consumer
if __name__ == "__main__":
    consumer = PostgresConsumer(
        bootstrap_servers="localhost:9092",
        topic="veritas-pages",
        group_id="veritas-postgres-consumer",
        db_config={
            "host": POSTGRES_HOST,
            "port": POSTGRES_PORT,
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "database": POSTGRES_DATABASE
        }
    )
    consumer.consume()