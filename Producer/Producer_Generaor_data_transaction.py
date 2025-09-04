import sys
import os
import time
import threading
import random
from datetime import datetime
from dotenv import load_dotenv

# Thêm sys.path để import local module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Generation_data.Fact_transaction import GenerationTranaction

# Kafka
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


class TransactionProducer:
    def __init__(self, conn_params, bootstrap_servers=None):
        if bootstrap_servers is None:
            bootstrap_servers = [
                '192.168.235.136:9092',
                '192.168.235.147:9092',
                '192.168.235.148:9092'
            ]
        self.conn_params = conn_params
        self.bootstrap_servers = bootstrap_servers
        self.producer = Producer({'bootstrap.servers': ','.join(bootstrap_servers)})
        # Truyền conn_params khi khởi tạo generator
        self.generator = GenerationTranaction(conn_params)
        self.running = True

    def create_topic_if_not_exists(self, topic_name, num_partitions=3, replication_factor=3):
        """Create Kafka topic if it does not exist"""
        admin_client = AdminClient({'bootstrap.servers': ','.join(self.bootstrap_servers)})
        existing_topics = admin_client.list_topics(timeout=10).topics

        if topic_name in existing_topics:
            print(f"✅ Topic '{topic_name}' already exists")
            return

        topic = NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        fs = admin_client.create_topics([topic])

        try:
            fs[topic_name].result()  # Wait for result
            print(f"🎉 Topic '{topic_name}' created successfully")
        except KafkaException as e:
            print(f"⚠️ Failed to create topic '{topic_name}': {e}")

    def transation_report(self, err, msg):
        """Callback xác nhận message gửi thành công hoặc thất bại"""
        if err is not None:
            print(f"❌ transation failed: {err}")
        else:
            print(f"✅ Message transation to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def producer_transaction(self, topic='transaction_data', rate=50):
        """Generate transaction data and send to Kafka"""
        while self.running:
            try:
                # Sinh dữ liệu giả từ generator
                data = self.generator.generator_data_transaction()

                # Convert thành string (nên dùng json.dumps nếu là dict)
                message = str(data)

                # Gửi vào Kafka
                self.producer.produce(
                    topic,
                    key=str(data['transaction_id']),
                    value=message,
                    callback=self.transation_report
                )

                # Flush định kỳ
                self.producer.poll(0)

                # Giới hạn tốc độ gửi (rate transaction/sec)
                time.sleep(1.0 / rate)

            except Exception as e:
                print(f"⚠️ Error producing message: {e}")
                time.sleep(1)

    def start_all_producers(self):
        # Tạo topic nếu chưa có
        self.create_topic_if_not_exists('transaction_data')

        thread = threading.Thread(target=self.producer_transaction, kwargs={'rate': 50})
        thread.daemon = True
        thread.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.running = False
            print("🛑 Stopping all producers...")


if __name__ == "__main__":
    load_dotenv()

    conn_params = {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS")
    }

    # Kiểm tra biến môi trường đã load chưa
    missing_vars = [k for k, v in conn_params.items() if v is None]
    if missing_vars:
        print(f"⚠️ Missing environment variables: {missing_vars}")
        sys.exit(1)

    print("✅ DB Connection params loaded from .env (password hidden)")

    producer = TransactionProducer(conn_params)
    producer.start_all_producers()
