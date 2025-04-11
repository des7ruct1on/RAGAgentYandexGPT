import logging
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from modules.agent import Agent
from modules.document_loader import DocumentLoader
from modules.vector_db import VectorDB
from modules.cache import CacheManager
from modules.messaging import KafkaMessaging
import config
from worker import Worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TelegramBot:
    def __init__(self, bot: Bot):
        logger.info("Initializing Telegram bot...")
        self.bot = bot
        self.dp = Dispatcher(self.bot)

        # Инициализация компонентов
        logger.info(f"Connecting to Redis at {config.REDIS_HOST}:{config.REDIS_PORT}")
        self.cache = CacheManager(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT
        )
        logger.info(f"Redis connection established")

        logger.info(f"Connecting to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS}")
        self.messaging = KafkaMessaging(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS
        )
        logger.info(f"creating kafka_producer")
        self.kafka_producer = self.messaging.create_producer()

        # RAG компоненты
        logger.info(f"Loading documents from {config.DATA_PATH}")
        self.document_loader = DocumentLoader(data_path=config.DATA_PATH)
        self.vector_db = VectorDB()
        self.agent = Agent(
            yandex_folder_id=config.YANDEX_FOLDER_ID,
            yandex_api_key=config.YANDEX_API_KEY,
            document_loader=self.document_loader,
            vector_db=self.vector_db
        )

        logger.info("Bot initialized successfully.")
        self._register_handlers()

        self.worker = Worker(bot=self.bot)

        loop = asyncio.get_event_loop()
        loop.create_task(self.worker.process_messages())

    def _register_handlers(self):
        @self.dp.message_handler(commands=['start', 'help'])
        async def send_welcome(message: types.Message):
            logger.info(f"Received command: /start or /help from {message.chat.id}")
            await message.reply("Привет! Я ассистент по путешествиям. Спроси меня о странах, достопримечательностях или лучших сезонах для отдыха!")


        @self.dp.message_handler()
        async def handle_message(message: types.Message):
            try:
                logger.info(f"Received message from {message.chat.id}: {message.text}")

                cache_key = self.cache.generate_cache_key(message.text)
                cached_response = self.cache.get(cache_key)

                if cached_response:
                    logger.info(f"Cache hit for message '{message.text}', sending cached response.")
                    await message.answer(cached_response['response'])
                    return

                logger.info(f"Cache miss for message '{message.text}', processing request...")

                msg = {
                    'chat_id': message.chat.id,
                    'query': message.text,
                    'message_id': message.message_id
                }

                logger.info(f"Sending message to Kafka topic '{config.KAFKA_REQUESTS_TOPIC}'")
                self.messaging.produce_message(
                    producer=self.kafka_producer,
                    topic=config.KAFKA_REQUESTS_TOPIC,
                    key=str(message.chat.id),
                    value=msg
                )

                logger.info(f"Message from {message.chat.id} sent to Kafka for processing.")
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Error processing message from {message.chat.id}: {str(e)}")
                await message.answer("Ошибка обработки запроса")

    async def handle_kafka_response(self, message_data: dict):
        """Обработка ответа из Kafka"""
        try:
            chat_id = message_data['chat_id']
            response = message_data['response']
            logger.info(f"Received response from Kafka for chat_id {chat_id}: {response}")

            # Отправка ответа пользователю в Telegram
            await self.bot.send_message(chat_id=chat_id, text=response)
        except Exception as e:
            logger.error(f"Failed to handle Kafka response: {str(e)}")

    def run(self):
        logger.info("Starting bot polling...")
        executor.start_polling(self.dp, skip_updates=True)
        logger.info("Bot polling started.")


if __name__ == '__main__':
    bot = Bot(token=config.TELEGRAM_TOKEN)

    telegram_bot = TelegramBot(bot=bot)

    telegram_bot.run()
