from pydantic import BaseModel, HttpUrl
from datetime import datetime
from uuid import UUID

class DocumentResponse(BaseModel):
    id: UUID
    title: str
    abstract: str
    published: datetime
    updated: datetime
    authors: list[str]
    link: HttpUrl
    source_category: str

class SemanticDocumentResponse(DocumentResponse):
    certainty: float

class KeywordDocumentResponse(DocumentResponse):
    score: float