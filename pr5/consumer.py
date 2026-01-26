import json
import sys
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from kafka import KafkaConsumer, KafkaProducer, TopicPartition


class DebeziumKafkaConsumer:
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.producer = None

    def check_kafka_connection(self) -> List[str]: # проверка подключения
        print(f'Проверка подключения к Kafka ({self.bootstrap_servers})...')
        try:
            # Создаем временного консюмера для проверки
            temp_consumer = KafkaConsumer(
                bootstrap_servers=[self.bootstrap_servers],
                consumer_timeout_ms=3000
            )
            topics = list(temp_consumer.topics())
            temp_consumer.close()

            print(f'Kafka доступен, найдено {len(topics)} топиков')
            return topics
        except Exception as e:
            print(f'Ошибка подключения к Kafka: {e}')
            raise

    def check_topic_exists(self, topic: str) -> bool:# проверка топика
        print(f'Проверка топика "{topic}"...')
        try:
            temp_consumer = KafkaConsumer(
                bootstrap_servers=[self.bootstrap_servers],
                consumer_timeout_ms=3000
            )

            if topic not in temp_consumer.topics():
                print(f'Топик "{topic}" не существует')
                temp_consumer.close()
                return False

            partitions = temp_consumer.partitions_for_topic(topic)
            print(f'Топик "{topic}" найден, партиции: {partitions}')
            temp_consumer.close()
            return True

        except Exception as e:
            print(f'Ошибка при проверке топика: {e}')
            return False

    def parse_debezium_message(self, message: Dict[str, Any]) -> Dict[str, Any]: # создание данных из топика
        try:
            payload = message.get('payload', {})
            op = payload.get('op', '')
            # Определяем операцию
            operation_map = {
                'c': 'CREATE',
                'u': 'UPDATE',
                'd': 'DELETE',
                'r': 'SNAPSHOT'
            }
            operation = operation_map.get(op, 'UNKNOWN')
            # Извлекаем данные
            if op == 'd':
                data = payload.get('before', {})
            else:
                data = payload.get('after', {})

            # Извлекаем метаданные
            source = message.get('source', {})
            ts_ms = message.get('ts_ms', 0)
            result = {
                'operation': operation,
                'table': source.get('table', ''),
                'schema': source.get('schema', ''),
                'database': source.get('db', ''),
                'timestamp': datetime.fromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d %H:%M:%S') if ts_ms else 'N/A',
                'data': data,
                'raw_message': message
            } #создание структуры сообщения

            return result
        except Exception as e:
            print(f'Ошибка парсинга сообщения: {e}')
            return {'error': str(e), 'raw_message': message}

    def print_user_message(self, parsed_msg: Dict[str, Any], msg_num: int): # вывод сообщения из таблицы users
        print(f"\nСообщение #{msg_num} [users]")
        print(f"Операция: {parsed_msg['operation']}")
        print(f"Время: {parsed_msg['timestamp']}")
        print(f"Данные:")
        data = parsed_msg['data']
        if data:
            print(f"ID: {data.get('id', 'N/A')}")
            print(f"Имя: {data.get('name', 'N/A')}")
            print(f"Email: {data.get('email', 'N/A')}")
            print(f"Created at: {data.get('created_at', 'N/A')}")
            if 'updated_at' in data:
                print(f"Updated at: {data.get('updated_at', 'N/A')}")
        else:
            print(f"[Нет данных]")

    def print_order_message(self, parsed_msg: Dict[str, Any], msg_num: int): # вывод сообщения из таблицы order
        print(f"\nСообщение #{msg_num} [orders]")
        print(f"Операция: {parsed_msg['operation']}")
        print(f"Время: {parsed_msg['timestamp']}")
        print(f"Данные:")

        data = parsed_msg['data']
        if data:
            print(f"ID: {data.get('id', 'N/A')}")
            print(f"User ID: {data.get('user_id', 'N/A')}")
            print(f"Товар: {data.get('product_name', 'N/A')}")
            print(f"Количество: {data.get('quantity', 'N/A')}")
            print(f"Цена: {data.get('price', 'N/A')}")
            print(f"Статус: {data.get('status', 'N/A')}")
            print(f"Created at: {data.get('created_at', 'N/A')}")
            if 'updated_at' in data:
                print(f"Updated at: {data.get('updated_at', 'N/A')}")
        else:
            print(f"[Нет данных]")

    def consume_debezium_topics(self, topics: List[str], timeout_seconds: int = 30): # чтение сообщений
        print(f'\nЗапуск чтения топиков: {topics}')
        # Создаем уникальный group_id
        timestamp = int(time.time())
        group_id = f'debezium-consumer-{timestamp}'

        try:# Создаем консюмер
            self.consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=[self.bootstrap_servers],
                group_id=group_id,
                auto_offset_reset='earliest',  # Читать с начала
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8') if x else '{}'),
                consumer_timeout_ms=2000,
                session_timeout_ms=10000,
                heartbeat_interval_ms=3000
            )

            print(f'Консюмер создан (group_id: {group_id})')
            time.sleep(2)  # Даем время на присоединение к группе

            message_count = 0
            start_time = time.time()

            print(f'\nОжидание сообщений (таймаут: {timeout_seconds} секунд)...')

            while time.time() - start_time < timeout_seconds:
                # Читаем сообщения
                records = self.consumer.poll(timeout_ms=2000)
                elapsed = int(time.time() - start_time)

                if not records:
                    print(f'Ожидание... ({elapsed}/{timeout_seconds} сек)')
                    continue

                # Обрабатываем сообщения
                for topic_partition, messages in records.items():
                    topic_name = topic_partition.topic
                    for message in messages:
                        message_count += 1
                        # Парсим сообщение Debezium
                        parsed_msg = self.parse_debezium_message(message.value)
                        # Выводим в зависимости от таблицы
                        if 'users' in topic_name:
                            self.print_user_message(parsed_msg, message_count)
                        elif 'orders' in topic_name:
                            self.print_order_message(parsed_msg, message_count)
                        else:
                            print(f"\nНеизвестный топик: {topic_name}")
                            print(f"Сообщение: {json.dumps(parsed_msg, indent=2, default=str)}")

                # Если получили сообщения и прошло больше 5 секунд, можно выйти
                if message_count > 0 and elapsed > 5:
                    print(f'\nПолучено {message_count} сообщений, завершение...')
                    break

            if message_count == 0:
                print(f'\nСообщений не получено за {timeout_seconds} секунд')

            self.consumer.close()
            print(f'\nЧтение завершено')

        except Exception as e:
            print(f'\nОшибка при чтении сообщений: {e}')
            if self.consumer:
                self.consumer.close()

    def get_topic_statistics(self, topic: str, limit: int = 10) -> Dict[str, Any]:
        print(f'Анализ топика "{topic}"...')

        try:
            # Создаем временного консюмера для анализа
            temp_consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[self.bootstrap_servers],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda x: json.loads(x.decode('utf-8') if x else '{}'),
                consumer_timeout_ms=5000
            )

            message_count = 0
            operations = {'CREATE': 0, 'UPDATE': 0, 'DELETE': 0, 'SNAPSHOT': 0}

            for message in temp_consumer:
                if message_count >= limit:
                    break

                parsed = self.parse_debezium_message(message.value)
                op = parsed.get('operation', 'UNKNOWN')
                if op in operations:
                    operations[op] += 1

                message_count += 1

            temp_consumer.close()

            stats = {
                'topic': topic,
                'total_messages_read': message_count,
                'operations': operations,
                'has_data': message_count > 0
            }

            print(f'   Проанализировано {message_count} сообщений')
            print(f'   Операции: {operations}')

            return stats

        except Exception as e:
            print(f'Ошибка анализа топика: {e}')
            return {'error': str(e)}

    def close(self):
        """Закрытие соединений"""
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.close()
        print('Соединения закрыты')


def main():
    # Конфигурация
    BOOTSTRAP_SERVERS = 'kafka1:9092'
    TOPICS = [
        'final-server.public.users',
        'final-server.public.orders'
    ]

    print("=" * 60)
    print("DEBEZIUM POSTGRESQL CDC CONSUMER")
    print("=" * 60)

    # Создаем экземпляр консюмера
    consumer = DebeziumKafkaConsumer(BOOTSTRAP_SERVERS)

    try:
        all_topics = consumer.check_kafka_connection()
        valid_topics = []
        for topic in TOPICS:
            if consumer.check_topic_exists(topic):
                valid_topics.append(topic)

        if not valid_topics:
            print(f'\nНе найдено ни одного из требуемых топиков: {TOPICS}')
            print(f'Доступные топики: {[t for t in all_topics if "final-server" in t]}')
            sys.exit(1)

        print(f'\nБудут прочитаны топики: {valid_topics}')
        print(f'\nСтатистика топиков:')
        for topic in valid_topics:
            stats = consumer.get_topic_statistics(topic, limit=5)
        consumer.consume_debezium_topics(valid_topics, timeout_seconds=30)

    except KeyboardInterrupt:
        print('\nПрервано пользователем')
    except Exception as e:
        print(f'\nКритическая ошибка: {e}')
        sys.exit(1)
    finally:
        consumer.close()

    print("\n" + "=" * 60)
    print("ПРОГРАММА ЗАВЕРШЕНА")
    print("=" * 60)


if __name__ == '__main__':

    main()
