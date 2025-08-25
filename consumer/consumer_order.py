from confluent_kafka import Producer, Consumer, KafkaError
import json
import time
from faker import Faker
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_CONFIG = {
    'bootstrap.servers': '192.168.235.136:9092',  # Địa chỉ Kafka broker
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
            
class SparkStreaming:
    def __init__(self, kafka_config, topics):
        self.spark = SparkSession.builder\
            .appName("KafkaSparkStreaming")\
            .getOrCreate()
        
        self.kafka_config = kafka_config
        self.topics = topics
        
    def start_streaming(self):
        """Bắt đầu streaming từ Kafka"""
        df = self.spark.readStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers", self.kafka_config['bootstrap.servers'])\
            .option("subscribe", ",".join(self.topics))\
            .load()
        
        # Chuyển đổi giá trị từ bytes sang string
        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        
        # Xử lý dữ liệu
        query = df.writeStream\
            .outputMode("append")\
            .format("console")\
            .start()
        
        query.awaitTermination()            
        
if __name__ == "__main__":
    topic_name = "order_events"
    
    consumer = KafkaConsumer(KAFKA_CONFIG, [topic_name])
    consumer.consume_messages()