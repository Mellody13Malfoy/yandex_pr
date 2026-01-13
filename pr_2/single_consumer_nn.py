#!/usr/bin/env python3
import json
import logging
import time
import os
import sys
from confluent_kafka import Consumer, KafkaError

# Создаем директорию для логов если нет
os.makedirs('/app/logs', exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'/app/logs/single_consumer_instance{os.getenv("APP_INSTANCE", "1")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(f'SingleConsumer-{os.getenv("APP_INSTANCE", "1")}')

class SingleMessageConsumer:
    def __init__(self, bootstrap_servers, topic, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

        # Конфигурация консьюмера (auto commit) - ПРАВИЛЬНЫЕ ПАРАМЕТРЫ
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': f'{group_id}-instance-{os.getenv("APP_INSTANCE", "1")}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,  # автоматический коммит
            'auto.commit.interval.ms': 5000,
            'session.timeout.ms': 30000
        }

        self.consumer = Consumer(self.config)
        logger.info(f"SingleMessageConsumer initialized")
        logger.info(f"   Topic: {topic}")
        logger.info(f"   Group ID: {self.config['group.id']}")
        logger.info(f"   Instance: {os.getenv('APP_INSTANCE', '1')}")
        logger.info(f"   Auto commit: ENABLED")
        logger.info(f"   Processing: One message at a time")

    def process_single_message(self, message):
        """Обработка одного сообщения"""
        try:
            if not message or not message.value():
               logger.warning("Received empty or None message")
               return

            if message.error():
               logger.error(f"Message error: {message.error()}")
               return
            data = json.loads(message.value().decode('utf-8'))

            # Симуляция обработки
            time.sleep(0.1)

            # Логирование
            logger.info(f"Processing single message:")
            logger.info(f"Partition: {message.partition()}")
            logger.info(f"Offset: {message.offset()}")
            logger.info(f"Type: {data.get('type')}")
            logger.info(f"ID: {data.get('id')}")

            # Случайная ошибка для теста
            message_number = data.get('id')
            if message_number % 15 == 0 and message_number is not None:
               raise Exception(f"Simulated error for message {data.get('message_number')}")

            logger.info(f"Processed message {data.get('message_number')}")

        except Exception as e:
            logger.error(f"Error: {str(e)}")

    def consume(self):
        """Основной цикл потребления (по одному сообщению)"""
        self.consumer.subscribe([self.topic])
        logger.info("Single consumer started...")

        msg_count = 0

        try:
            while True:
                # Poll для одного сообщения
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                msg_count += 1
                self.process_single_message(msg)

                # Авто-коммит происходит автоматически
                if msg_count % 10 == 0:
                    logger.info(f"Processed {msg_count} messages total")

        except KeyboardInterrupt:
            logger.info("Consumer stopped")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
        finally:
            self.consumer.close()
            logger.info(f"Total messages processed: {msg_count}")

def main():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9092')
    topic = os.getenv('KAFKA_TOPIC', 'my-replicated-topic')

    os.makedirs('/app/logs', exist_ok=True)

    consumer = SingleMessageConsumer(bootstrap_servers, topic, 'single-message-group')
    consumer.consume()

if __name__ == '__main__':
    main()
