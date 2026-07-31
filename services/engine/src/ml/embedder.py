import ollama
import json
from sentence_transformers import SentenceTransformer
from generated.intelligence_events import PropertyIngestedEvent

model = SentenceTransformer('BAAI/bge-small-en-v1.5')

def build_intelligence_context(payload: PropertyIngestedEvent) -> str:
    payload_json = json.dumps(payload.to_dict())
    
    prompt = f"generate a highly dense, semantic paragraph describing this real estate property for a search engine embedding. do not use bullet points, just one paragraph: {payload_json}"
    
    response = ollama.chat(model='phi3', messages=[
        {
            'role': 'user',
            'content': prompt,
        },
    ])
    
    return response['message']['content']

def generate_embedding(context: str) -> list[float]:
    return model.encode(context).tolist()
