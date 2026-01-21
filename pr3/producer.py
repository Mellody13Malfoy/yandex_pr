import json
import time
from kafka import KafkaProducer
import os
import sys

def main():
    bootstrap_servers = 'kafka1:9092'
    topic = 'messages'
    print(f'Подключение')
    try:
        producer = KafkaProducer(bootstrap_servers=[bootstrap_servers], value_serializer=lambda v: json.dumps(v).encode('utf-8'),retries=5,request_timeout_ms=5000)
        print(f'Подключение успешно')
        test_messages = [ {'message_id':f'test_{int(time.time())}' , 'sender_id': 1, 'receiver_id':4, 'text': 'testing message number 4, spam','timestamp': time.time()},
                          {'message_id': f'test_{int(time.time())}', 'sender_id': 2, 'receiver_id': 1, 'text': 'testing message number 4', 'timestamp': time.time()}
                          ]
        print(f'Отправка сообщения')
        for i, message in enumerate(test_messages,1):
            future = producer.send(topic,value=message)
            record_metadata = future.get(timeout=10)
        producer.close()
    except Exception as e:
        print(f'ошибка {e}')


if __name__ == '__main__':
    main()