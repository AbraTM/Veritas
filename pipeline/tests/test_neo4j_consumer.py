from consumers.neo4j_consumer import Neo4jConsumer
from dotenv import load_dotenv
import os

load_dotenv()
NEO4J_URI = os.environ["NEO4J_URI"]
NEO4J_USER = os.environ["NEO4J_USER"]
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]

if __name__ == "__main__":
    consumer = Neo4jConsumer(
        bootstrap_servers="localhost:9092",
        topic="veritas-pages",
        group_id="veritas-neo4j-consumer",
        neo4j_url=NEO4J_URI,
        neo4j_auth=(NEO4J_USER, NEO4J_PASSWORD)
    )

    consumer.consume()