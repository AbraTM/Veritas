import os
import weaviate
import weaviate.classes as wvc
import weaviate.auth as wv_auth
from dotenv import load_dotenv
from typing import Generator

load_dotenv()
WEAVIATE_URL=os.environ["WEAVIATE_URL"]
WEAVIATE_API_KEY=os.environ["WEAVIATE_API_KEY"]

client = None

def init_weaviate_client():
    global client
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
        print("Disconnected from Weaviate.")

def get_weaviate_client() -> Generator:
    yield client