import os
import weaviate
import weaviate.classes as wvc
import weaviate.auth as wv_auth
from dotenv import load_dotenv
from typing import Generator

load_dotenv()
WEAVIATE_URL=os.environ["WEAVIATE_URL"]
WEAVIATE_API_KEY=os.environ["WEAVIATE_API_KEY"]

client: weaviate.WeaviateClient | None = None

def init_weaviate_client():
    global client
    if client is None:
        client = weaviate.connect_to_weaviate_cloud(
            cluster_url=WEAVIATE_URL,
            auth_credentials=wv_auth.AuthApiKey(WEAVIATE_API_KEY)
        )
        if not client.is_ready():
            raise ConnectionError("Failed to connect to Weaviate.")
    return client

def close_weaviate_client():
    global client
    if client:
        client.close()
        client = None
        print("Disconnected from Weaviate.")

def get_weaviate_client() -> weaviate.WeaviateClient:
    if client is None:
        raise RuntimeError("Weaviate client not initialized. Did you forget lifespan?")
    return client