import json
import sys
import time

from kafka import KafkaConsumer, TopicPartition


def kafka_check(bootstrap_servers):
    print(f'Проверка подключение кафики')
    try:
        consumer = KafkaConsumer(bootstrap_servers=[bootstrap_servers], consumer_timeout_ms=3000)
        topics = list(consumer.topics())
        consumer.close()
        print(f'Кафка доступен')
        return topics
    except Exception as e:
        print(f'ошибка {e}')

def topic_detal(bootstrap_servers,topic):
    print(f'Проверка топика')
    try:
        consumer = KafkaConsumer(bootstrap_servers=[bootstrap_servers], consumer_timeout_ms=3000)
        if topic not in consumer.topics():
            print(f'Топик не существует')
            consumer.close()
            return False
        partitions = consumer.partitions_for_topic(topic)
        print(f'Топик найден, партиции: {partitions}')
        consumer.close()
        return True
    except Exception as e:
        print(f' ошибка при проверки топика')

def main():
    bootstrap_servers = 'kafka1:9092'
    topic = 'filtered_messages'
    print(f'Подключение')
    timestamp= int(time.time())
    group_id=f'filtered-group-{timestamp}'
    topics = kafka_check(bootstrap_servers)
    if not topics or not topic_detal(bootstrap_servers,topic):
        sys.exit(1)
    print(f'Старт констьюмера')
    try:
        consumer = KafkaConsumer(topic,bootstrap_servers=[bootstrap_servers], group_id=group_id, auto_offset_reset='earliest',
                                 enable_auto_commit=True, consumer_timeout_ms=10000,
                                 session_timeout_ms=10000,heartbeat_interval_ms=3000)
        time.sleep(2)
        message_count = 0
        timeout=15
        start_time = time.time()
        try:
            while time.time() - start_time <timeout:
                records = consumer.poll(timeout_ms=2000)
                elapsed = int(time.time()-start_time)
                if not records:
                    print(f'Ожидаем сообщения')
                    continue
                for tp,messages in records.items():
                    for message in messages:
                        message_count +=1
                        message_value = message.value.decode('utf-8')
                        #print(f'{message_value}, {type(message_value)}')
                        if len(message_value) != 0:
                            mess = message_value.replace('{','').replace('}','').replace('\\"','').split(',')
                            text_mes = ['ID: ', 'от: ', 'кому: ','Текст: ']
                            for i in range(len(mess)-1):
                                text_mes[i] += mess[i].split(':')[1]
                            print(f'Сообщение номер: {message_count}')
                            print(f'{" ,".join(text_mes)}')
                            #print(f'Сообщение номер {message_count}, ID: {message_value.get("message_id", "N/A")}, от {message_value.get("sender_id", "N/A")}, кому {message_value.get("receiver_id", "N/A")}')
                            #print(f' Текст: {message_value.get("text", "N/A")}')
                        else:
                            print(f'Сообщение пустое')
                if message_count >0 and elapsed >5:
                    print(f'получены сообщения, сеанс заверщшается')
                    consumer.close()
                    break
        except Exception as e:
            print(f'ошибка {e}')
    except Exception as e:
        print(f'ошибка {e}')




if __name__ == '__main__':
    main()