from confluent_kafka import Producer, Consumer, KafkaError
import json
import time
from faker import Faker
import random
from datetime import datetime, timedelta

KAFKA_CONFIG = {
    'bootstrap.servers': '192.169.235.143:9092',  # Địa chỉ Kafka broker
    'client.id': 'python-client'
}
class KafkaConsumer:
    def __init__(self, config, topics):
        consumer_config = config.copy()
        consumer_config.update({
            'group.id': 'python-consumer-group',
            'auto.offset.reset': 'earliest'  # Đọc từ đầu topic
        })
        
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe(topics)
    
    def consume_messages(self, timeout=1.0):
        """Consume messages từ Kafka topics"""
        try:
            while True:
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f'End of partition reached {msg.topic()}/{msg.partition()}')
                    else:
                        print(f'Error: {msg.error()}')
                else:
                    # Xử lý message
                    key = msg.key().decode('utf-8') if msg.key() else None
                    value = msg.value().decode('utf-8')
                    
                    print(f'Received message:')
                    print(f'  Topic: {msg.topic()}')
                    print(f'  Partition: {msg.partition()}')
                    print(f'  Offset: {msg.offset()}')
                    print(f'  Key: {key}')
                    print(f'  Value: {value}')
                    print('-' * 50)
                    
        except KeyboardInterrupt:
            print('Consumer stopped by user')
        finally:
            self.consumer.close()

if __name__ == "__main__":
    topic_name = "delivery-topic"
    
    consumer = KafkaConsumer(KAFKA_CONFIG, [topic_name])
    consumer.consume_messages()