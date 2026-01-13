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
        logging.FileHandler(f'/app/logs/batch_consumer_instance{os.getenv("APP_INSTANCE", "1")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(f'BatchConsumer-{os.getenv("APP_INSTANCE", "1")}')

class BatchMessageConsumer:
    def __init__(self, bootstrap_servers, topic, group_id, batch_size=10):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.batch_size = batch_size

        # Конфигурация консьюмера (ручной коммит) - ПРАВИЛЬНЫЕ ПАРАМЕТРЫ
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': f'{group_id}-instance-{os.getenv("APP_INSTANCE", "1")}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # РУЧНОЙ коммит
            'fetch.wait.max.ms': 5000,    # Ждем до 5 секунд для набора батча
            'fetch.message.max.bytes': 1048576,  # 1 MB max message size
            'max.partition.fetch.bytes': 1048576,  # 1 MB per partition
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000
        }

        self.consumer = Consumer(self.config)
        logger.info(f"BatchMessageConsumer initialized")
        logger.info(f"Topic: {topic}")
        logger.info(f"Group ID: {self.config['group.id']}")
        logger.info(f"Instance: {os.getenv('APP_INSTANCE', '1')}")
        logger.info(f"Batch size: {batch_size}")
        logger.info(f"Manual commit: ENABLED")
        logger.info(f"Processing: Loop through batch, commit once")

    def process_batch(self, messages):
        """Обработка батча сообщений в цикле"""
        logger.info(f"Processing batch of {len(messages)} messages")

        success_count = 0
        error_count = 0

        # Обработка каждого сообщения в цикле
        for i, msg in enumerate(messages, 1):
            try:
                data = json.loads(msg.value().decode('utf-8'))

                # Быстрая симуляция обработки
                time.sleep(0.05)

                logger.debug(f"   [{i}/{len(messages)}] Processing: {data.get('id', 'unknown')}")

                # Случайная ошибка для теста
                if data.get('message_number', 0) % 20 == 0:
                    raise Exception(f"Batch error for message {data.get('message_number')}")

                success_count += 1

            except Exception as e:
                error_count += 1
                logger.error(f"   Error in message {data.get('id', 'unknown')}: {str(e)}")

        logger.info(f"Batch result: {success_count} success, {error_count} errors")
        return success_count, error_count

    def consume(self):
        """Основной цикл потребления (пачками по 10)"""
        self.consumer.subscribe([self.topic])
        logger.info("Batch consumer started...")
        logger.info("Strategy: Collect min 10 messages, process in loop, commit once")

        batch_count = 0
        total_messages = 0

        try:
            while True:
                # Собираем батч из минимум 10 сообщений
                batch = []
                start_time = time.time()

                while len(batch) < self.batch_size:
                    msg = self.consumer.poll(1.0)

                    if msg is None:
                        # Если набрали хоть что-то и прошло время
                        if len(batch) > 0 and (time.time() - start_time) > 5:
                            logger.info(f"Timeout reached, processing {len(batch)} messages")
                            break
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        logger.error(f"Consumer error: {msg.error()}")
                        continue

                    batch.append(msg)

                if not batch:
                    continue

                batch_count += 1
                logger.info(f"🔨 Batch #{batch_count}: collected {len(batch)} messages")

                # Обработка батча
                success, errors = self.process_batch(batch)
                total_messages += success

                try:
                    self.consumer.commit(asynchronous=False)
                    logger.info(f"Committed offsets for batch #{batch_count}")
                except Exception as e:
                    logger.error(f"Commit failed: {e}")

                logger.info(f"--- Batch #{batch_count} completed ---")

                # Статистика каждые 5 батчей
                if batch_count % 5 == 0:
                    logger.info(f"Statistics: {batch_count} batches, {total_messages} messages total")

        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
        finally:
            self.consumer.close()
            logger.info(f"Total batches processed: {batch_count}")
            logger.info(f"Total messages processed: {total_messages}")

def main():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9092')
    topic = os.getenv('KAFKA_TOPIC', 'my-replicated-topic')

    # Создание директории для логов
    os.makedirs('/app/logs', exist_ok=True)

    # Запуск консьюмера
    consumer = BatchMessageConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id='batch-message-group',
        batch_size=10
    )
    consumer.consume()

if __name__ == '__main__':
    main()
