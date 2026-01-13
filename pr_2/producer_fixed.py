#!/usr/bin/env python3
import json
import time
import logging
from datetime import datetime
from confluent_kafka import Producer
import os
import sys

# Создаем директорию для логов
os.makedirs('/app/logs', exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - PRODUCER - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'/app/logs/producer.log'),  
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('Producer')

class KafkaProducerApp:
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'kafka-producer-app',
            'acks': 'all'
        }
        self.producer = Producer(self.config)
        logger.info(f"Producer initialized. Topic: {topic}")

    def delivery_report(self, err, msg):
        if err:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

    def produce_messages(self):
        """PUSH модель: продюсер сам отправляет сообщения"""
        message_count = 0

        logger.info("Starting message production (PUSH model)...")

        try:
            while True:
                message_count += 1

                # Создаем сообщение
                message = {
                    'id': message_count,
                    'timestamp': datetime.now().isoformat(),
                    'data': f'Test message {message_count}',
                    'type': 'test'
                }

                # Отправляем (PUSH)
                self.producer.produce(
                    topic=self.topic,
                    key=str(message_count % 3).encode(),
                    value=json.dumps(message),
                    callback=self.delivery_report
                )

                logger.info(f"Pushed message {message_count}: {message['type']}")

                # Периодический flush
                if message_count % 10 == 0:
                    self.producer.flush()
                    logger.info(f"Sent {message_count} messages")

                # Задержка между сообщениями
                time.sleep(0.5)

        except KeyboardInterrupt:
            logger.info("Producer stopped")
        except Exception as e:
            logger.error(f"Error: {e}")
        finally:
            self.producer.flush()

def main():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9092')
    topic = os.getenv('KAFKA_TOPIC', 'my-replicated-topic')

    # Создаем директорию для логов
    os.makedirs('/app/logs', exist_ok=True)

    producer = KafkaProducerApp(bootstrap_servers, topic)
    producer.produce_messages()

if __name__ == '__main__':
    main()
