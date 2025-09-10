from sentence_transformers import SentenceTransformer

_model = None

def get_embedding_model():
    global _model
    if not _model:
        _model = SentenceTransformer("sentence-transformers/all-mpnet-base-v2")
    return _model