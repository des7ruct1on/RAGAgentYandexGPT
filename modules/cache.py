import redis
import json
from typing import Optional, Any
import logging

logger = logging.getLogger(__name__)

class CacheManager:
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0):
        self.redis = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True
        )
        try:
            self.redis.ping()
            logger.info("Connected to Redis successfully")
        except redis.ConnectionError:
            logger.error("Failed to connect to Redis")

    def get(self, key: str) -> Optional[Any]:
        """Получение данных из кэша"""
        try:
            cached = self.redis.get(key)
            return json.loads(cached) if cached else None
        except Exception as e:
            logger.error(f"Cache get error: {str(e)}")
            return None

    def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        """Сохранение данных в кэш"""
        try:
            self.redis.setex(key, ttl, json.dumps(value))
            return True
        except Exception as e:
            logger.error(f"Cache set error: {str(e)}")
            return False

    def generate_cache_key(self, query: str) -> str:
        """Генерация ключа кэша на основе запроса"""
        return f"rag_response:{hash(query)}"