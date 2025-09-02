from consumers.postgres_consumer import PostgresConsumer
from dotenv import load_dotenv
import logging
import os

load_dotenv()
POSTGRES_HOST=os.environ["POSTGRES_HOS"]
POSTGRES_USER=os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD=os.environ["POSTGRES_PASSWORD"]
POSTGRES_PORT=os.environ["POSTGRES_PORT"]
POSTGRES_DATABASE=os.environ["POSTGRES_DATABASE"]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
    
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