from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import time

KAFKA_BROKER = '192.168.235.143:9092'
TOPIC_NAME = 'test'

def create_topic():
    """Tạo topic nếu chưa tồn tại"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER,
            client_id='python-admin'
        )
        
        topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        
        print(f"✅ Topic '{TOPIC_NAME}' created successfully")
    except Exception as e:
        if "TopicExistsError" in str(type(e)):
            print(f"⚠️ Topic '{TOPIC_NAME}' đã tồn tại")
        else:
            print(f"❌ Error creating topic: {e}")

def test_minimal_producer():
    """Producer tối giản để test kết nối"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='1',
            retries=1
        )
        
        print("✅ Producer created successfully")
        
        test_data = {"test": "hello kafka", "timestamp": time.time()}
        future = producer.send(TOPIC_NAME, test_data)
        result = future.get(timeout=10)
        
        print(f"✅ Message sent successfully: {result}")
        producer.close()
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    create_topic()
    if test_minimal_producer():
        print("🎉 Kafka connection OK! Có thể chạy producer chính.")
    else:
        print("⚠️ Gửi message thất bại")
