from neo4j import AsyncGraphDatabase
from dotenv import load_dotenv
from typing import AsyncGenerator
import os

load_dotenv()
NEO4J_URI=os.environ["NEO4J_URI"]
NEO4J_USER=os.environ["NEO4J_USER"]
NEO4J_PASSWORD=os.environ["NEO4J_PASSWORD"]

driver = None

async def init_neo4j_driver():
    global driver
    driver = AsyncGraphDatabase.driver(
        NEO4J_URI, 
        auth=(NEO4J_USER, NEO4J_PASSWORD)
    )
    await driver.verify_connectivity()
    return driver

async def close_neo4j_driver():
    global driver
    if driver:
        await driver.close()

async def get_neo4j_session() -> AsyncGenerator:
    async with driver.session() as session:
        yield session
    