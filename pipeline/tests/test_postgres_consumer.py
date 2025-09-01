from consumers.postgres_consumer import PostgresConsumer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
    
if __name__ == "__main__":
    consumer = PostgresConsumer(
        bootstrap_servers="localhost:9092",
        topic="veritas-pages",
        group_id="veritas-postgres-consumer",
        db_config={
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "0000",
            "database": "Veritas"
        }
    )
    consumer.consume()