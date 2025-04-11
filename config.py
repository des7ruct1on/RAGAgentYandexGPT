import os
from dotenv import load_dotenv

load_dotenv()
YANDEX_FOLDER_ID = os.getenv("YANDEX_FOLDER_ID")
YANDEX_API_KEY = os.getenv("YANDEX_API_KEY")

# Telegram Bot Token
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# Paths
DATA_PATH = os.getenv("DATA_PATH", "data")

# Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
CACHE_TTL = int(os.getenv("CACHE_TTL", 86400))

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_REQUESTS_TOPIC = os.getenv("KAFKA_REQUESTS_TOPIC", "rag_requests")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "rag_workers")
KAFKA_RESPONSES_TOPIC = os.getenv("KAFKA_RESPONSES_TOPIC", "rag_requests")