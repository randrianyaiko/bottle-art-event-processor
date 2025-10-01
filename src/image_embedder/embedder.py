from fastembed import ImageEmbedding
from dotenv import load_dotenv
import os
import tempfile
import requests
from pathlib import Path
import numpy as np

# Load environment variables
load_dotenv()
MODEL_NAME = os.getenv('FASTEMBED_MODEL_NAME')
CACHE_DIR = os.getenv('FASTEMBED_CACHE_DIR')
embedder = ImageEmbedding(MODEL_NAME, cache_dir=CACHE_DIR)

def embed_image(url: str) -> list:
    """Download an image from a URL and return its embedding as float16."""
    
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_file = Path(tmpdir) / "temp_image"
        response = requests.get(url)
        response.raise_for_status()
        temp_file.write_bytes(response.content)
        
        embedding = next(embedder.embed(temp_file))
    
    # Convert to float16 for reduced memory
    return np.array(embedding, dtype=np.float16).tolist()

# Example usage:
# embedding = embed_image("https://example.com/image.jpg")
# print(embedding)
