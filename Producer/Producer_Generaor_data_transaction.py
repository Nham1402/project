import sys
import os
import time
import threading
import random
import json
from datetime import datetime
from dotenv import load_dotenv

# Thêm sys.path để import local module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Generation_data.Fact_transaction import GenerationTranaction

# Kafka
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import logging

# ================== Logging ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TransactionProducer:
    def __init__(self, conn_params, bootstrap_servers=None):
        if bootstrap_servers is None:
            bootstrap_servers = [
                '192.168.235.136:9092',
                # '192.168.235.147:9092',  # Comment out nếu chỉ có 1 broker
                # '192.168.235.148:9092'   # Comment out nếu chỉ có 1 broker
            ]
        
        self.conn_params = conn_params
        self.bootstrap_servers = bootstrap_servers
        
        # Producer configuration với retry và timeout
        producer_config = {
            'bootstrap.servers': ','.join(bootstrap_servers),
            'acks': 'all',                    # Chờ tất cả replicas confirm
            'retries': 3,                     # Retry 3 lần nếu fail
            'retry.backoff.ms': 1000,         # Đợi 1s giữa các retry
            'request.timeout.ms': 30000,      # Timeout 30s
            'delivery.timeout.ms': 60000,     # Tổng timeout 60s
            'batch.size': 16384,              # Batch size
            'linger.ms': 10,                  # Đợi 10ms để batch messages
            'compression.type': 'snappy'      # Nén dữ liệu
        }
        
        try:
            self.producer = Producer(producer_config)
            logger.info("✅ Kafka Producer initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka Producer: {e}")
            raise
            
        # Truyền conn_params khi khởi tạo generator
        try:
            self.generator = GenerationTranaction(conn_params)
            logger.info("✅ Transaction Generator initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Transaction Generator: {e}")
            raise
            
        self.running = True
        self.message_count = 0

    def create_topic_if_not_exists(self, topic_name, num_partitions=1, replication_factor=1):
        """Create Kafka topic if it does not exist"""
        try:
            admin_client = AdminClient({'bootstrap.servers': ','.join(self.bootstrap_servers)})
            existing_topics = admin_client.list_topics(timeout=10).topics

            if topic_name in existing_topics:
                logger.info(f"✅ Topic '{topic_name}' already exists")
                return True

            # Adjust replication factor based on available brokers
            topic = NewTopic(
                topic=topic_name, 
                num_partitions=num_partitions, 
                replication_factor=replication_factor
            )
            
            fs = admin_client.create_topics([topic])
            fs[topic_name].result()  # Wait for result
            logger.info(f"🎉 Topic '{topic_name}' created successfully")
            return True
            
        except KafkaException as e:
            logger.error(f"⚠️ Failed to create topic '{topic_name}': {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error creating topic: {e}")
            return False

    def delivery_report(self, err, msg):
        """Callback xác nhận message gửi thành công hoặc thất bại"""
        if err is not None:
            logger.error(f"❌ Message delivery failed: {err}")
        else:
            self.message_count += 1
            if self.message_count % 100 == 0:  # Log mỗi 100 messages
                logger.info(f"✅ Delivered {self.message_count} messages - Latest to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def serialize_data(self, data):
        """Convert data to JSON string with proper formatting"""
        try:
            # Ensure datetime objects are serialized properly
            if isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, datetime):
                        data[key] = value.isoformat()
            
            # Convert to JSON string (not Python repr string)
            return json.dumps(data, ensure_ascii=False, default=str)
            
        except Exception as e:
            logger.error(f"❌ Error serializing data: {e}")
            return json.dumps({"error": "serialization_failed", "data": str(data)})

    def producer_transaction(self, topic='transaction_data', rate=5):
        """Generate transaction data and send to Kafka"""
        logger.info(f"🚀 Starting transaction producer for topic '{topic}' at {rate} msg/sec")
        
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        while self.running:
            try:
                # Sinh dữ liệu giả từ generator
                data = self.generator.generator_data_transaction()

                # Serialize data properly as JSON
                message = self.serialize_data(data)

                # Gửi vào Kafka
                self.producer.produce(
                    topic,
                    key=str(data.get('transaction_id', 'unknown')),
                    value=message,
                    callback=self.delivery_report
                )

                # Flush định kỳ (non-blocking)
                self.producer.poll(0)

                # Reset error counter on success
                consecutive_errors = 0

                # Giới hạn tốc độ gửi (rate transaction/sec)
                time.sleep(1.0 / rate)

            except KeyError as e:
                consecutive_errors += 1
                logger.error(f"⚠️ Data generation error (missing key): {e}")
                time.sleep(1)
                
            except KafkaException as e:
                consecutive_errors += 1
                logger.error(f"⚠️ Kafka error: {e}")
                time.sleep(2)
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"⚠️ Unexpected error producing message: {e}")
                time.sleep(1)
                
            # Stop if too many consecutive errors
            if consecutive_errors >= max_consecutive_errors:
                logger.error(f"❌ Too many consecutive errors ({consecutive_errors}). Stopping producer.")
                self.running = False
                break

        # Final flush
        logger.info("🔄 Flushing remaining messages...")
        self.producer.flush(timeout=30)
        logger.info(f"✅ Producer stopped. Total messages sent: {self.message_count}")

    def start_all_producers(self, topic='transaction_data', rate=5):
        """Start producer with error handling"""
        try:
            # Tạo topic nếu chưa có
            if not self.create_topic_if_not_exists(topic):
                logger.error("❌ Cannot create topic. Exiting...")
                return

            # Start producer thread
            thread = threading.Thread(
                target=self.producer_transaction, 
                kwargs={'topic': topic, 'rate': rate}
            )
            thread.daemon = True
            thread.start()
            
            logger.info("🎯 Producer started. Press Ctrl+C to stop...")

            # Main loop với status reporting
            start_time = time.time()
            while True:
                time.sleep(10)  # Report every 10 seconds
                elapsed = time.time() - start_time
                rate_actual = self.message_count / elapsed if elapsed > 0 else 0
                logger.info(f"📊 Status: {self.message_count} messages sent, {rate_actual:.2f} msg/sec average")
                
        except KeyboardInterrupt:
            logger.info("🛑 Received stop signal...")
        except Exception as e:
            logger.error(f"❌ Error in producer: {e}")
        finally:
            self.running = False
            logger.info("🔄 Cleaning up...")
            self.producer.flush(timeout=10)
            logger.info("✅ Producer stopped cleanly")

    def test_connection(self):
        """Test Kafka connection"""
        try:
            admin_client = AdminClient({'bootstrap.servers': ','.join(self.bootstrap_servers)})
            metadata = admin_client.list_topics(timeout=5)
            logger.info(f"✅ Kafka connection successful. Available topics: {list(metadata.topics.keys())}")
            return True
        except Exception as e:
            logger.error(f"❌ Kafka connection failed: {e}")
            return False


if __name__ == "__main__":
    # Load environment variables
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
        logger.error(f"⚠️ Missing environment variables: {missing_vars}")
        sys.exit(1)

    logger.info("✅ DB Connection params loaded from .env")

    try:
        # Initialize producer
        producer = TransactionProducer(conn_params)
        
        # Test connection first
        if not producer.test_connection():
            logger.error("❌ Cannot connect to Kafka. Please check broker status.")
            sys.exit(1)
            
        # Start producer with configurable rate
        rate = int(os.getenv("PRODUCER_RATE", 2))  # Default 2 msg/sec
        topic = os.getenv("KAFKA_TOPIC", "transaction_data")
        
        logger.info(f"🚀 Starting producer: topic={topic}, rate={rate} msg/sec")
        producer.start_all_producers(topic=topic, rate=rate)
        
    except Exception as e:
        logger.error(f"❌ Failed to start producer: {e}")
        sys.exit(1)