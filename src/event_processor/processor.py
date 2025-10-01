import threading
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from typing import Dict, List, Optional
from scipy.stats import zscore

from src.vectorcache.products import ProductEmbeddingCache
from src.featurestore.featurestore import InteractionCache
from src.image_embedder.embedder import embed_image
from src.vectorstore.vectorstore import QdrantDenseClient 
from dotenv import load_dotenv
import os

load_dotenv()
# ------------------- Services -------------------
store = QdrantDenseClient()
cachedvectors = ProductEmbeddingCache()
featurestore = InteractionCache()

# ------------------- Config -------------------
EVENT_WEIGHTS = {
    "VIEW": 1,
    "ADD_TO_CART": 2,
    "UPDATE_CART_QUANTITY": 1,
    "REMOVE_FROM_CART": 0,
    "ORDER": 5,
}
EMA_ALPHA = os.getenv("EMA_ALPHA", 0.5)

# ------------------- Caches -------------------
user_cache: Dict[str, Dict[str, float]] = {}  # {user_id: {product_id: score}}
user_pref_cache: Dict[str, Dict[str, List[str]]] = {}  # {user_id: {"liked": [...], "disliked": [...]}}

user_locks: Dict[str, threading.Lock] = {}

def get_user_lock(user_id: str) -> threading.Lock:
    if user_id not in user_locks:
        user_locks[user_id] = threading.Lock()
    return user_locks[user_id]

# ------------------- ThreadPool for async image embedding -------------------
IMAGE_EXECUTOR = ThreadPoolExecutor(max_workers=5)

# ------------------- Helper: update EMA -------------------
def update_interaction(user_id: str, product_id: str, event_type: str) -> None:
    weight = EVENT_WEIGHTS.get(event_type)
    if weight is None:
        return

    lock = get_user_lock(user_id)
    with lock:
        interactions = user_cache.get(user_id, {})
        old_score = interactions.get(product_id, 0.0)
        new_score = (1 - EMA_ALPHA) * old_score + EMA_ALPHA * weight
        interactions[product_id] = new_score
        user_cache[user_id] = interactions

        # compute likes/dislikes using min-max scaling between 0 and 1
        products = list(interactions.keys())
        scores = np.array([interactions[p] for p in products])

        if len(scores) >= 2:  # need at least 2 to scale
            min_score, max_score = scores.min(), scores.max()
            # avoid division by zero
            if max_score == min_score:
                scaled_scores = np.ones_like(scores)  # all products get 1
            else:
                scaled_scores = (scores - min_score) / (max_score - min_score)

            n = len(scores)
            cutoff = max(1, int(n * 0.3))
            liked = [products[i] for i in np.argsort(scaled_scores)[-cutoff:]]
            disliked = [products[i] for i in np.argsort(scaled_scores)[:cutoff]]
        else:
            liked = list(interactions.keys())
            disliked = []

        user_pref_cache[user_id] = {"liked": liked, "disliked": disliked}

        # persist to featurestore
        featurestore.store({
            "user_id": user_id,
            "interactions": interactions,
            "liked": liked,
            "disliked": disliked,
            "timestamp": datetime.utcnow().isoformat()
        })

# ------------------- Product creation -------------------
def create_product(event: dict):
    product_id = event["product_id"]
    image_url = event["details"]["image"]

    def task():
        embedding = embed_image(image_url)
        store.insert_point(point_id=product_id, dense_vector=embedding)
        cachedvectors.store({"id": product_id, "vectors": embedding})
        print(f"[INFO] Stored product {product_id} in Qdrant")
    IMAGE_EXECUTOR.submit(task)

# ------------------- Event handler -------------------
def event_handler(event: dict):
    activity_type = event.get("activity_type")
    user_id = event.get("user_id")
    product_id = event.get("product_id")

    if activity_type == "CREATE_PRODUCT":
        create_product(event)
    elif activity_type in EVENT_WEIGHTS and user_id and product_id:
        update_interaction(user_id, product_id, activity_type)

# ------------------- Get user preferences -------------------
def get_user_preferences(user_id: str) -> Dict[str, List[str]]:
    return user_pref_cache.get(user_id, {"liked": [], "disliked": []})
