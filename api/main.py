from fastapi import FastAPI
from contextlib import asynccontextmanager
from dbs.neo4j.config import init_neo4j_driver, close_neo4j_driver
from dbs.weaviate.config import init_weaviate_client, close_weaviate_client

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_neo4j_driver()
    init_weaviate_client()
    yield
    await close_neo4j_driver()
    close_weaviate_client()

app = FastAPI(lifespan=lifespan)

@app.get("/")
def home():
    return {
        "service": "Veritas",
        "status": "OK"
    }