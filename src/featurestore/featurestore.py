import redis
import json
import os
from dotenv import load_dotenv
load_dotenv()

class InteractionCache:
    def __init__(self, host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), password=os.getenv("REDIS_PASSWORD"), use_aof=False):
        """
        :param host: Redis host
        :param port: Redis port
        :param password: Redis password
        :param use_aof: Whether to enable append-only file (AOF) persistence
        """
        self.redis = redis.Redis(host=host, port=port, password=password, decode_responses=True)
        
        if use_aof:
            # Enable AOF persistence at runtime
            self.redis.config_set("appendonly", "yes")
            print("AOF persistence enabled")

    def store(self, data: dict):
        key = f"user-{data['user_id']}"
        self.redis.set(key, json.dumps(data))
        # Force RDB snapshot immediately
        self.redis.save()
        print(f"Stored data under key: {key} and forced persistence")

    def retrieve(self, id: int|str):
        key = f"user-{id}"
        stored_data = self.redis.get(key)
        if stored_data:
            return json.loads(stored_data)#['vector']
        return None
    
    

    