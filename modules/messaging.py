from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
import json
import logging

logger = logging.getLogger(__name__)

class KafkaMessaging:
    def __init__(self, bootstrap_servers: str = 'localhost:9093'):
        self.bootstrap_servers = bootstrap_servers
        logger.info(f"Initialized KafkaMessaging with bootstrap servers: {self.bootstrap_servers}")

    def create_producer(self):
        """Создание Kafka producer"""
        logger.info(f"Creating Kafka producer with bootstrap servers: {self.bootstrap_servers}")
        return Producer({
            'bootstrap.servers': self.bootstrap_servers,
            'message.max.bytes': 10485760  # 10MB
        })

    def create_topic_if_not_exists(self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
        """Создание темы, если она не существует"""
        logger.info(f"Checking if topic '{topic_name}' exists...")
        admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        try:
            topic_metadata = admin_client.list_topics(timeout=5)
            if topic_name not in topic_metadata.topics:
                logger.info(f"Topic '{topic_name}' does not exist, creating it...")
                topic = NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
                fs = admin_client.create_topics([topic])

                for topic, f in fs.items():
                    try:
                        f.result()
                        logger.info(f"Topic '{topic}' created successfully.")
                    except Exception as e:
                        logger.error(f"Failed to create topic '{topic}': {str(e)}")
            else:
                logger.info(f"Topic '{topic_name}' already exists.")
        except Exception as e:
            logger.error(f"Error checking or creating topic: {str(e)}")

    def create_consumer(self, group_id: str, topics: list):
        """Создание Kafka consumer"""
        logger.info(f"Creating Kafka consumer for group: {group_id}, subscribing to topics: {topics}")
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'latest'
        })
        consumer.subscribe(topics)
        return consumer

    @staticmethod
    def delivery_report(err, msg):
        """Callback для подтверждения доставки сообщения"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            # Логируем содержимое сообщения
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
            logger.debug(f"Message key: {msg.key().decode('utf-8')}")
            logger.debug(f"Message value: {msg.value().decode('utf-8')}")

    def produce_message(self, producer, topic: str, key: str, value: dict):
        """Отправка сообщения в Kafka"""
        logger.info(f"Producing message to topic '{topic}' with key: {key}")
        try:
            producer.produce(
                topic=topic,
                key=key,
                value=json.dumps(value),
                callback=self.delivery_report
            )
            producer.poll(0)
            logger.info(f"Message sent successfully to topic '{topic}' with key: {key}")
        except Exception as e:
            logger.error(f"Failed to produce message to topic '{topic}' with key: {key}, error: {str(e)}")

    def consume_messages(self, consumer, timeout: float = 1.0):
        """Чтение сообщений из Kafka"""
        try:
            msg = consumer.poll(timeout)
            if msg is None:
                logger.debug("No messages received during this poll cycle.")
                return None

            logger.info(f"Polled message from topic '{msg.topic()}'. Message key: {msg.key()}")

            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                return None

            consumer.commit(msg)
            message_data = {
                'topic': msg.topic(),
                'key': msg.key().decode('utf-8'),
                'value': json.loads(msg.value().decode('utf-8'))
            }
            logger.info(f"Consumed message: {message_data}")
            return message_data
        except Exception as e:
            logger.error(f"Consume error: {str(e)}")
            return None
