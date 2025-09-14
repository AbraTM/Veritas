from fastapi import APIRouter, HTTPException, Depends
from dbs.postgres.config import get_postgres_db
from models.document import DocumentResponse
from dbs.postgres.config import get_postgres_db
import asyncpg

router = APIRouter(prefix="/document", tags=["Documents", "Relational Data"])

# Get Document with particular ID
@router.get("/{doc_id}", response_model=DocumentResponse)
async def getDocs(
    doc_id: str,
    pg_db: asyncpg.Connection = Depends(get_postgres_db),

):
    rows = await pg_db.fetch(
        """ 
            SELECT * FROM pages LIMIT 5;
        """
    )
    print(rows)
    return {
        'message': 'ok'
    }
    
