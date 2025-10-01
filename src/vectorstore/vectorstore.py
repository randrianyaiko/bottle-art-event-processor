import os
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from qdrant_client import QdrantClient, models
from qdrant_client.http import models as rest_models

class QdrantDenseClient:
    def __init__(self):
        # Load environment variables from .env
        load_dotenv()

        qdrant_url = os.getenv('QDRANT_URL')
        api_key = os.getenv('QDRANT_API_KEY')
        collection_name = os.getenv('QDRANT_COLLECTION_NAME', 'dense_collection')
        dense_vector_name = os.getenv('QDRANT_DENSE_VECTOR_NAME', 'dense')
        dense_size_str = os.getenv('QDRANT_DENSE_SIZE')
        distance_name = os.getenv('QDRANT_DISTANCE', 'Cosine')

        if qdrant_url is None:
            raise ValueError("QDRANT_URL must be set in environment")

        try:
            dense_size = int(dense_size_str)
            if dense_size <= 0:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError("QDRANT_DENSE_SIZE must be a positive integer")

        try:
            distance = getattr(models.Distance, distance_name.upper())
        except AttributeError:
            raise ValueError(f"Invalid distance {distance_name}. Valid options: {[d.name for d in models.Distance]}")

        self.client = QdrantClient(url=qdrant_url, api_key=api_key)
        self.collection_name = collection_name
        self.dense_vector_name = dense_vector_name
        self.dense_size = dense_size
        self.distance = distance

        # Check or create collection
        if not self.client.collection_exists(collection_name=self.collection_name):
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config={
                    self.dense_vector_name: models.VectorParams(
                        size=self.dense_size,
                        distance=self.distance
                    )
                },
            )
            print(f"Created dense-only collection '{self.collection_name}'")
        else:
            print(f"Collection '{self.collection_name}' already exists")

    def insert_point(
        self,
        point_id: str,
        dense_vector: List[float],
        payload: Optional[Dict[str, Any]] = None
    ):
        """
        Insert a single dense vector point into Qdrant.
        """
        if len(dense_vector) != self.dense_size:
            raise ValueError(f"Dense vector size mismatch: expected {self.dense_size}, got {len(dense_vector)}")

        point = rest_models.PointStruct(
            id=point_id,
            vector={self.dense_vector_name: dense_vector},
            payload=payload or {}
        )
        self.client.upsert(
            collection_name=self.collection_name,
            points=[point],
            wait=True
        )
        print(f"Inserted point '{point_id}' with dense vector")

    def update_point(
        self,
        point_id: str,
        dense_vector: Optional[List[float]] = None,
        payload: Optional[Dict[str, Any]] = None
    ):
        """
        Update a dense vector point in Qdrant. If dense_vector is None, only payload is updated.
        """
        existing = self.client.retrieve(
            collection_name=self.collection_name,
            ids=[point_id],
            with_payload=True,
            with_vectors=True
        )
        if not existing or len(existing) == 0:
            raise ValueError(f"No point found with id={point_id}")

        vector_data = {self.dense_vector_name: dense_vector} if dense_vector else None

        point = rest_models.PointStruct(
            id=point_id,
            vector=vector_data,
            payload=payload or {}
        )
        self.client.upsert(
            collection_name=self.collection_name,
            points=[point],
            wait=True
        )
        print(f"Updated point '{point_id}'")

    def delete_point(self, point_id: str):
        self.client.delete(
            collection_name=self.collection_name,
            point_id=point_id,
            wait=True
        )
        print(f"Deleted point '{point_id}'")

    def get_points(
        self,
        point_ids: List[str],
        with_payload: bool = True,
        with_vectors: bool = True
    ) -> Optional[rest_models.Record]:
        resp = self.client.retrieve(
            collection_name=self.collection_name,
            ids=point_ids,
            with_payload=with_payload,
            with_vectors=with_vectors
        )
        if not resp:
            print(f"No points found for ids={point_ids}")
            return None
        return resp
