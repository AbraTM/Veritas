from pydantic import BaseModel, HttpUrl
from datetime import datetime

class DocumentResponse(BaseModel):
    id: str
    title: str
    abstract: str
    published: datetime
    updated: datetime
    authors: list[str]
    link: HttpUrl
    source_category: str

    class Config:
        from_attributes=True