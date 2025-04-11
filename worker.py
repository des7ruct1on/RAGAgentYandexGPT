import logging
import asyncio
from modules.agent import Agent
from modules.document_loader import DocumentLoader
from modules.vector_db import VectorDB
from modules.cache import CacheManager
from modules.messaging import KafkaMessaging
import config
from aiogram import Bot

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Worker:
    def __init__(self, bot: Bot):
        logger.info("Initializing Worker...")
        self.bot = bot
        self.cache = CacheManager(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT
        )
        logger.info(f"CacheManager initialized with Redis host: {config.REDIS_HOST}, port: {config.REDIS_PORT}")

        self.messaging = KafkaMessaging(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS
        )
        logger.info(f"KafkaMessaging initialized with bootstrap servers: {config.KAFKA_BOOTSTRAP_SERVERS}")

        self.messaging.create_topic_if_not_exists(config.KAFKA_REQUESTS_TOPIC)
        logger.info(f"Kafka topic '{config.KAFKA_REQUESTS_TOPIC}' checked/created")

        self.kafka_producer = self.messaging.create_producer()
        logger.info(f"Created kafka_producer: {self.kafka_producer}")

        self.kafka_consumer = self.messaging.create_consumer(
            group_id=config.KAFKA_GROUP_ID,
            topics=[config.KAFKA_REQUESTS_TOPIC]
        )
        logger.info(f"Kafka consumer created for group {config.KAFKA_GROUP_ID} and topic {config.KAFKA_REQUESTS_TOPIC}")

        # RAG компоненты
        self.document_loader = DocumentLoader(data_path=config.DATA_PATH)
        logger.info(f"DocumentLoader initialized with data path: {config.DATA_PATH}")

        self.vector_db = VectorDB()
        logger.info("VectorDB initialized")

        self.agent = Agent(
            yandex_folder_id=config.YANDEX_FOLDER_ID,
            yandex_api_key=config.YANDEX_API_KEY,
            document_loader=self.document_loader,
            vector_db=self.vector_db
        )
        logger.info("Agent initialized with Yandex credentials and data sources")

    async def process_messages(self):
        logger.info("Worker started processing messages...")
        while True:
            try:
                msg = self.messaging.consume_messages(self.kafka_consumer)
                if msg:
                    # logger.info(f"Received message from Kafka: {msg}")
                    await self._handle_request(msg['value'])  # Асинхронная обработка запроса
                else:
                    logger.debug("No message received during this poll cycle")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error during message processing: {str(e)}")

    async def _handle_request(self, request: dict):
        try:
            query = request.get('query')
            chat_id = request.get('chat_id')

            if not query or not chat_id:
                logger.warning(f"Skipping request due to missing 'query' or 'chat_id': {request}")
                return

            logger.info(f"Handling request with query: {query} from chat_id: {chat_id}")

            cache_key = self.cache.generate_cache_key(query)
            logger.info(f"Generated cache key: {cache_key}")

            cached_response = self.cache.get(cache_key)
            if cached_response:
                logger.info(f"Cache hit for query: {query}")
                response = cached_response['response']
            else:
                logger.info(f"Cache miss for query: {query}, generating response...")
                response = self.agent.generate_response(query)
                logger.info(f"Saving response to cache for query: {query}")
                self.cache.set(
                    key=cache_key,
                    value={'response': response},
                    ttl=config.CACHE_TTL
                )

            logger.info(f"Sending response to chat_id {chat_id}")
            await self.bot.send_message(
                chat_id=chat_id,
                text=response,
                parse_mode="Markdown"
            )

            self.messaging.produce_message(
                producer=self.kafka_producer,
                topic=config.KAFKA_RESPONSES_TOPIC,
                key=str(chat_id),
                value={'response': response, 'chat_id': chat_id}
            )

        except Exception as e:
            logger.error(f"Failed to process request: {str(e)}")
            try:
                if request.get('chat_id'):
                    await self.bot.send_message(
                        chat_id=request['chat_id'],
                        text="Произошла ошибка при обработке запроса"
                    )
            except Exception as inner_e:
                logger.error(f"Failed to send error message to chat {request.get('chat_id')}: {str(inner_e)}")

