from fastapi import FastAPI
from contextlib import asynccontextmanager
from dbs.postgres.config import init_postgres, close_postgres
from dbs.neo4j.config import init_neo4j_driver, close_neo4j_driver
from dbs.weaviate.config import init_weaviate_client, close_weaviate_client
from routes.document import router as DocumentRouter

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_postgres()
    # await init_neo4j_driver()
    init_weaviate_client()
    yield
    await close_postgres()
    # await close_neo4j_driver()
    close_weaviate_client()

app = FastAPI(lifespan=lifespan)

app.include_router(router=DocumentRouter, prefix="/api/v1")

@app.get("/")
def home():
    return {
        "service": "Veritas",
        "status": "OK"
    }