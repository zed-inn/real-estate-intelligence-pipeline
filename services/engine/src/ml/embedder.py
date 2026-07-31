from sentence_transformers import SentenceTransformer

model = SentenceTransformer('BAAI/bge-small-en-v1.5')

def generate_embedding(context: str) -> list[float]:
    return model.encode(context).tolist()
