import os
import asyncpg
from dotenv import load_dotenv
from typing import AsyncGenerator, Optional

load_dotenv()
POSTGRES_URL = os.environ["POSTGRES_URL"]

pool: Optional[asyncpg.pool] = None

async def init_postgres():
    global pool
    pool = await asyncpg.create_pool(dsn=POSTGRES_URL, min_size=1, max_size=10)
    if not pool:
        raise Exception("Failed to initialize Postgres Connection.")
    

async def close_postgres():
    global pool
    if pool:
        await pool.close()

async def get_postgres_db() -> AsyncGenerator[asyncpg.Connection, None]:
    async with pool.acquire() as conn:
        yield conn