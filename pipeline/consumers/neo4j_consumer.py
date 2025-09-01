from consumers.base_consumer import BaseConsumer
from neo4j import GraphDatabase
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Neo4jConsumer(BaseConsumer):
    def __init__(self, bootstrap_servers, topic, group_id, neo4j_url, neo4j_auth):
        super().__init__(bootstrap_servers, topic, group_id)
        self.neo4j_url = neo4j_url
        self.neo4j_auth = neo4j_auth
        self.driver = None
    
    def connect_to_neo4j(self):
        if not self.neo4j_url and not self.neo4j_auth:
            logger.error("Connection to Neo4j Failed..")
            return 
        try:
            self.driver = GraphDatabase.driver(self.neo4j_url, auth=self.neo4j_auth)
            self.driver.verify_connectivity()
            logger.info("Connection successfully estabhlished to Neo4j AuraDB.")
        except Exception as e:
            logger.error(f"Failed to estabhlish connection to Neo4j AuraDB.\n {e}")

    def disconnect_from_neo4j(self):
        if not self.driver:
            logger.error("No connection existing to disconnect from.")
        self.driver.close()
        logger.info("Disconnected successfully from Neo4j AuraDB.")

    def process_item(self, item):
        if not self.driver:
            logging.error("No Neo4j connectiona available to process item.")
            return
        
        try:
            with self.driver.session() as session:
                session.execute_write(self._store_item_tx, item)
            logger.info(f"Item stored successfully: {item.get('title')} from source {item.get('source_category')}.")
        except Exception as e:
            logger.error(f"Error storing item: {item.get('title')} from source {item.get('source_category')}.\n {e}")

    @staticmethod
    def _store_item_tx(tx, item):
        query = """
            MERGE (p:Paper {id: $id})
                ON CREATE SET 
                    p.title = $title,
                    p.abstract = $abstract,
                    p.published = datetime($published), 
                    p.updated = datetime($updated), 
                    p.link = $link
                ON MATCH SET
                    p.title = $title,
                    p.abstract = $abstract,
                    p.updated = datetime($updated),
                    p.link = $link
            WITH p
            UNWIND $authors as author
                MERGE (a:Author {name: author})
                MERGE (a)-[:WROTE]->(p)
            WITH p
            MERGE (c:Category {name: $source_category})
            MERGE (p)-[:BELONGS_TO]->(c)
        """

        tx.run(
            query,
            id=item.get("id"),
            title=item.get("title"),
            abstract=item.get("abstract"),
            published=item.get("published"),
            updated=item.get("updated"),
            link=item.get("link"),
            authors=item.get("authors", []),
            source_category=item.get("source_category")
        )
    
    def consume(self):
        self.connect_to_neo4j()
        try:
            super().consume()
        except Exception as e:
            self.disconnect_from_neo4j()