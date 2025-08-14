from confluent_kafka import Producer, Consumer, KafkaError
import json
import time
from faker import Faker
import random
from datetime import datetime, timedelta

class DeliveryDataGenerator:
    def __init__(self, locale='vi_VN'):
        self.fake = Faker(locale)
        self.delivery_statuses = ['Pending', 'In Transit', 'Delivered', 'Failed', 'Returned']
        self.regions = ['Hanoi', 'Ho Chi Minh', 'Da Nang', 'Hai Phong', 'Can Tho', 'Hue', 'Nha Trang', 'Vung Tau']
    
    def generate_delivery_record(self):
        """Tạo một bản ghi delivery theo schema đã định"""
        created_at = self.fake.date_time_between(start_date='-30d', end_date='now')
        delivery_date = created_at + timedelta(days=random.randint(1, 7))
        
        record = {
            "order_id": f"ORD-{self.fake.random_number(digits=8)}",
            "customer_name": self.fake.name(),
            "address": self.fake.address().replace('\n', ', '),
            "phone": self.fake.phone_number(),
            "delivery_status": random.choice(self.delivery_statuses),
            "package_weight_kg": round(random.uniform(0.5, 50.0), 2),
            "delivery_date": delivery_date.strftime('%Y-%m-%d %H:%M:%S'),
            "created_at": created_at.strftime('%Y-%m-%d %H:%M:%S'),
            "region": random.choice(self.regions)
        }
        
        return record
    
    # def generate_multiple_records(self, count=10):
    #     """Tạo nhiều bản ghi delivery"""
    #     return [self.generate_delivery_record() for _ in range(count)]
    
KAFKA_CONFIG = {
    'bootstrap.servers': '192.168.235.143:9092',  # Địa chỉ Kafka broker
    'client.id': 'python-client'
}

class KafkaProducer:
    def __init__(self, config):
        self.producer = Producer(config)
    
    def delivery_callback(self, err, msg):
        """Callback được gọi khi message được gửi thành công hoặc thất bại"""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to topic {msg.topic()} partition {msg.partition()}')
    
    def send_message(self, topic, key, value):
        """Gửi message tới Kafka topic"""
        try:
            # Serialize value thành JSON nếu là dict
            if isinstance(value, dict):
                value = json.dumps(value)
            
            self.producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=self.delivery_callback
            )
            
            # Đợi để đảm bảo message được gửi
            self.producer.flush()
            
        except Exception as e:
            print(f'Error sending message: {e}')      
              
if __name__ == "__main__":
    topic_name = "delivery-topic"
    
    # Tạo data generator
    data_generator = DeliveryDataGenerator()
    
    # Tạo Producer và gửi fake delivery data
    print("=== PRODUCER - Sending Fake Delivery Data ===")
    producer = KafkaProducer(KAFKA_CONFIG)
    
    
    try:
        while True:
            record = data_generator.generate_delivery_record()
            
            print(f"Record {i+1}:")
            print(f"  Order ID: {record['order_id']}")
            print(f"  Customer: {record['customer_name']}")
            print(f"  Address: {record['address'][:50]}..." if len(record['address']) > 50 else f"  Address: {record['address']}")
            print(f"  Phone: {record['phone']}")
            print(f"  Status: {record['delivery_status']}")
            print(f"  Weight: {record['package_weight_kg']} kg")
            print(f"  Delivery Date: {record['delivery_date']}")
            print(f"  Created At: {record['created_at']}")
            print(f"  Region: {record['region']}")
                
                # Gửi tới Kafka
            producer.send_message(
                    topic=topic_name,
                    key=record['order_id'],
                    value=record
                )
            print(f"  ✓ Sent to Kafka topic '{topic_name}'")
            print("-" * 50)
            time.sleep(1)
    except Exception as e:
        print(f"Error generating or sending fake records: {e}")