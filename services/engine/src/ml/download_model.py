import logging
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def pre_download():
    logger.info("downloading baai/bge-small-en-v1.5 from huggingface...")
    model = SentenceTransformer('BAAI/bge-small-en-v1.5')
    logger.info("model downloaded successfully and cached!")

if __name__ == "__main__":
    pre_download()
