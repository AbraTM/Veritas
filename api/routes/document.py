from fastapi import APIRouter, HTTPException, Depends, Query
from models.document import DocumentResponse, SemanticDocumentResponse, KeywordDocumentResponse
from dbs.postgres.config import get_postgres_db
from dbs.weaviate.config import get_weaviate_client
from datetime import datetime
from weaviate.classes.query import MetadataQuery
from utils.vectorizer import get_model
import asyncpg
import weaviate

router = APIRouter(prefix="/document", tags=["Documents"])

# Relational Postgres DB Data
# Get Document with particular ID
@router.get("/relational/{doc_id}", response_model=DocumentResponse, tags=["Relational Data (Postgres DB)"])
async def get_document(
    doc_id: str,
    pg_db: asyncpg.Connection = Depends(get_postgres_db),

):
    row = await pg_db.fetchrow(
        """ 
            SELECT * FROM pages 
            WHERE id = $1;
        """,
        doc_id
    )
    if not row:
        raise HTTPException(status_code=404, detail="No Document with this ID.")
    return dict(row)
    
# Get Document with query params (Meta Data Filtering)
@router.get("/relational/", response_model=list[DocumentResponse], tags=["Relational Data (Postgres DB)"])
async def get_document_metadata(
    title: str | None = Query(None),
    published_on: datetime | None = Query(None),
    published_after: datetime | None = Query(None),
    published_before: datetime | None = Query(None),
    updated_on: datetime | None = Query(None),
    updated_after: datetime | None = Query(None),
    updated_before: datetime | None = Query(None),
    authors: list[str] | None = Query(None),
    source_category: str | None = Query(None),
    pg_db: asyncpg.Connection = Depends(get_postgres_db)
):
    query = "SELECT * FROM pages WHERE TRUE"
    params = []

    # Getting Document based on it's title
    if title:
        params.append(f"%{title}%")
        query += f" AND title ILIKE ${len(params)}"
    
    # Getting Document based on it's source_category
    if source_category:
        params.append(source_category.lower())
        query += f" AND source_category = ${len(params)}"

    # Getting Document based on the name of authors
    if authors:
        params.append(authors)
        query += f" AND authors && ${len(params)}"

    # Getting Document based on publishing time
    if published_on:
        params.append(published_on)
        query += f" AND published = ${len(params)}"
    
    # Getting Document based on ranges of publishing times
    if published_after and published_before:
        params.append(published_after)
        params.append(published_before)
        query += f" AND published BETWEEN ${len(params) - 1} AND ${len(params)}"
    elif published_after:
        params.append(published_after)
        query += f" AND published >= ${len(params)}"
    elif published_before:
        params.append(published_before)
        query += f" AND published <= ${len(params)}"

    # Getting Document based on updating time
    if updated_on:
        params.append(updated_on)
        query += f" AND updated = ${len(params)}"
    
    # Getting Document based on ranges of updating time
    if updated_after and updated_before:
        params.append(updated_after)
        params.append(updated_before)
        query += f" AND updated BETWEEN ${len(params) - 1} AND ${len(params)}"
    elif updated_after:
        params.append(updated_after)
        query += f" AND updated >= ${len(params)}"
    elif updated_before:
        params.append(updated_before)
        query += f" AND updated <= ${len(params)}"
    
    rows = await pg_db.fetch(query, *params)
    return [dict(row) for row in rows]


# Semantic Search from Weaviate Vector DB
@router.get("/vector/", response_model=list[SemanticDocumentResponse], tags=["Vector Search"])
async def semantic_search(
    semantic_search: str | None = Query(None),
    weaviate_db: weaviate.WeaviateClient = Depends(get_weaviate_client)
):
    documents = weaviate_db.collections.use("Page")
    model = get_model()
    search_vector = model.encode(semantic_search).tolist()
    response = documents.query.near_vector(
        near_vector=search_vector,
        return_metadata=MetadataQuery(certainty=True)
    )
    result = []
    for o in response.objects:
        result.append({
            "id": str(o.uuid),
            "title": o.properties.get("title"),
            "abstract": o.properties.get("abstract"),
            "published": o.properties.get("published"),
            "updated": o.properties.get("updated"),
            "authors": o.properties.get("authors"),
            "link": o.properties.get("link"),
            "source_category": o.properties.get("source_category"),
            "certainty": o.metadata.certainty
        })
    return result

# Keyword Search from Weaviate Vector DB
@router.get("/keyword/", response_model=list[KeywordDocumentResponse], tags=["Vector Search"])
async def keyword_search(
    args: list[str] | None = Query(None),
    weaviate_db: weaviate.WeaviateClient = Depends(get_weaviate_client)
):
    documents = weaviate_db.collections.use("Page")
    response = documents.query.bm25(
        query=" ".join(args),
        return_metadata=MetadataQuery(score=True, explain_score=True)
    )
    result = []
    for o in response.objects:
        result.append({
            "id": str(o.uuid),
            "title": o.properties.get("title"),
            "abstract": o.properties.get("abstract"),
            "published": o.properties.get("published"),
            "updated": o.properties.get("updated"),
            "authors": o.properties.get("authors"),
            "link": o.properties.get("link"),
            "source_category": o.properties.get("source_category"),
            "score": o.metadata.score,
        })
    return result