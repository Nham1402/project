from confluent_kafka import Producer, KafkaError
from faker import Faker
import json
import random
import time
import threading
import signal
import sys
from datetime import datetime, timedelta

fake = Faker(['vi_VN', 'en_US'])

class ConfluentKafkaProducer:
    def __init__(self):
        """Khởi tạo Confluent Kafka Producer"""
        
        # Cấu hình producer
        conf = {
            'bootstrap.servers': '192.168.235.143:9092,192.168.235.144:9092,192.168.235.145:9092',
            'client.id': 'delivery-producer',
            
            # Cấu hình cho reliability
            'acks': '1',                    # Chờ leader ack
            'retries': 5,                   # Retry 5 lần
            'retry.backoff.ms': 100,        # 100ms giữa các retry
            'request.timeout.ms': 30000,    # 30s timeout
            'delivery.timeout.ms': 120000,  # 2 phút total timeout
            
            # Cấu hình cho performance
            'batch.size': 16384,            # 16KB batch
            'linger.ms': 10,                # Đợi 10ms để batch
            'compression.type': 'snappy',   # Nén dữ liệu
            'buffer.memory': 33554432,      # 32MB buffer
            
            # Error handling
            'enable.idempotence': 'false',  # Tắt idempotence để đơn giản
            'max.in.flight.requests.per.connection': 1,  # Đảm bảo order
        }
        
        try:
            self.producer = Producer(conf)
            print("✅ Confluent Kafka Producer đã được tạo thành công")
        except Exception as e:
            print(f"❌ Không thể tạo producer: {e}")
            sys.exit(1)
        
        self.topic_name = "delivery_orders"
        self.running = True
        self.sent_count = 0
        self.error_count = 0
        
        # Setup signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """Xử lý signal để dừng gracefully"""
        print(f"\n🛑 Nhận signal {signum}. Đang dừng producer...")
        self.running = False
    
    def delivery_callback(self, err, msg):
        """Callback khi message được delivered"""
        if err is not None:
            self.error_count += 1
            print(f"❌ Message delivery failed: {err}")
        else:
            self.sent_count += 1
            if self.sent_count % 500 == 0:
                print(f"✅ Đã gửi {self.sent_count} messages (Partition: {msg.partition()}, Offset: {msg.offset()})")
    
    def fake_delivery_data(self):
        """Tạo dữ liệu fake"""
        now = datetime.now()
        random_days_ago = random.randint(0, 7)
        delivery_date = now - timedelta(days=random_days_ago, hours=random.randint(0, 23))
        created_date = delivery_date - timedelta(hours=random.randint(1, 48))
        
        vietnamese_addresses = [
            "123 Nguyễn Huệ, Quận 1, TP.HCM",
            "456 Lê Lợi, Quận Hai Bà Trưng, Hà Nội", 
            "789 Trần Phú, Quận Hải Châu, Đà Nẵng",
            "321 Võ Văn Tần, Quận 3, TP.HCM",
            "654 Hoàng Diệu, Quận 4, TP.HCM"
        ]
        
        vietnamese_names = [
            "Nguyễn Văn An", "Trần Thị Bình", "Lê Hoàng Cường",
            "Phạm Thu Dung", "Hoàng Minh Đức", "Vũ Thị Hương"
        ]
        
        return {
            "order_id": fake.uuid4(),
            "customer_name": random.choice(vietnamese_names),
            "address": random.choice(vietnamese_addresses),
            "phone": fake.phone_number(),
            "delivery_status": random.choice([
                "pending", "confirmed", "shipping", "shipped", 
                "out_for_delivery", "delivered", "canceled", "returned"
            ]),
            "package_weight_kg": round(random.uniform(0.1, 25.0), 2),
            "delivery_date": delivery_date.isoformat(),
            "created_at": created_date.isoformat(),
            "region": random.choice(["hcm", "hn", "dn"]),
            "delivery_fee": round(random.uniform(15000, 50000), 0),
            "package_value": round(random.uniform(100000, 2000000), 0),
            "delivery_type": random.choice(["standard", "express", "same_day"])
        }
    
    def send_message(self, data):
        """Gửi một message"""
        try:
            # Convert data to JSON
            json_data = json.dumps(data, ensure_ascii=False)
            
            # Sử dụng region làm key để partition
            key = data['region']
            
            # Produce message
            self.producer.produce(
                topic=self.topic_name,
                key=key.encode('utf-8'),
                value=json_data.encode('utf-8'),
                callback=self.delivery_callback
            )
            
            # Poll để trigger callbacks (non-blocking)
            self.producer.poll(0)
            
            return True
            
        except Exception as e:
            print(f"❌ Lỗi khi gửi message: {e}")
            self.error_count += 1
            return False
    
    def run_continuous(self, messages_per_second=100):
        """Chạy producer liên tục"""
        print(f"🚀 Bắt đầu gửi {messages_per_second} messages/second")
        print(f"📡 Topic: {self.topic_name}")
        print("🔄 Nhấn Ctrl+C để dừng\n")
        
        interval = 1.0 / messages_per_second
        last_stats_time = time.time()
        
        try:
            while self.running:
                batch_start = time.time()
                
                # Gửi messages
                for _ in range(min(messages_per_second, 100)):  # Max 100 per loop
                    if not self.running:
                        break
                        
                    data = self.fake_delivery_data()
                    self.send_message(data)
                    
                    time.sleep(interval)
                
                # Flush messages định kỳ
                self.producer.flush(timeout=1.0)
                
                # In stats mỗi 30 giây
                if time.time() - last_stats_time >= 30:
                    self.print_stats()
                    last_stats_time = time.time()
                
        except KeyboardInterrupt:
            print("\n🛑 Nhận Ctrl+C...")
        finally:
            self.cleanup()
    
    def run_batch_mode(self, batch_size=1000, batches=10, delay_between_batches=2):
        """Chạy theo chế độ batch"""
        print(f"🚀 Batch mode: {batches} batches × {batch_size} messages")
        print(f"⏱️ Delay giữa batches: {delay_between_batches}s\n")
        
        try:
            for batch_num in range(batches):
                if not self.running:
                    break
                
                print(f"📤 Đang gửi batch {batch_num + 1}/{batches}...")
                batch_start = time.time()
                
                # Gửi batch
                for i in range(batch_size):
                    if not self.running:
                        break
                        
                    data = self.fake_delivery_data()
                    self.send_message(data)
                    
                    # Poll mỗi 100 messages
                    if i % 100 == 0:
                        self.producer.poll(0)
                
                # Flush batch này
                remaining = self.producer.flush(timeout=30.0)
                if remaining > 0:
                    print(f"⚠️ {remaining} messages chưa được gửi trong batch {batch_num + 1}")
                
                batch_time = time.time() - batch_start
                print(f"✅ Batch {batch_num + 1} hoàn thành trong {batch_time:.2f}s")
                
                # Delay giữa batches
                if batch_num < batches - 1:
                    time.sleep(delay_between_batches)
                
        except KeyboardInterrupt:
            print("\n🛑 Nhận Ctrl+C...")
        finally:
            self.cleanup()
    
    def print_stats(self):
        """In thống kê"""
        total = self.sent_count + self.error_count
        success_rate = (self.sent_count / total * 100) if total > 0 else 0
        print(f"📊 Stats: ✅ {self.sent_count} sent, ❌ {self.error_count} errors, Success rate: {success_rate:.1f}%")
    
    def cleanup(self):
        """Cleanup resources"""
        print("\n🧹 Đang cleanup...")
        
        # Flush tất cả messages còn lại
        print("💾 Flushing remaining messages...")
        remaining = self.producer.flush(timeout=30.0)
        
        if remaining > 0:
            print(f"⚠️ {remaining} messages không thể gửi được")
        else:
            print("✅ Tất cả messages đã được gửi")
        
        self.print_stats()
        print("✅ Producer đã dừng hoàn toàn")

def test_connection():
    """Test kết nối trước khi chạy producer chính"""
    print("🧪 Testing Confluent Kafka connection...")
    
    conf = {
        'bootstrap.servers': '192.168.235.143:9092',
        'client.id': 'test-producer'
    }
    
    try:
        producer = Producer(conf)
        
        # Gửi test message
        test_data = {"test": "connection", "timestamp": time.time()}
        json_data = json.dumps(test_data)
        
        producer.produce(
            topic='delivery_orders',
            value=json_data.encode('utf-8')
        )
        
        # Flush và đợi kết quả
        remaining = producer.flush(timeout=10.0)
        
        if remaining == 0:
            print("✅ Connection test thành công!")
            return True
        else:
            print(f"❌ Connection test failed: {remaining} messages không gửi được")
            return False
            
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

def main():
    """Main function"""
    
    # Test connection trước
    if not test_connection():
        print("❌ Không thể kết nối Kafka. Kiểm tra lại setup!")
        return
    
    producer = ConfluentKafkaProducer()
    
    print("\nChọn chế độ chạy:")
    print("1. Continuous (liên tục)")
    print("2. Batch mode (theo đợt)")
    print("3. High throughput test (tốc độ cao)")
    print("4. Test mode (100 messages)")
    
    choice = input("Nhập lựa chọn (1-4): ").strip()
    
    if choice == "1":
        rate = int(input("Messages per second (mặc định 100): ") or "100")
        producer.run_continuous(messages_per_second=rate)
        
    elif choice == "2":
        batch_size = int(input("Batch size (mặc định 1000): ") or "1000")
        batches = int(input("Số batches (mặc định 10): ") or "10")
        delay = float(input("Delay giữa batches (s, mặc định 2): ") or "2")
        producer.run_batch_mode(batch_size=batch_size, batches=batches, delay_between_batches=delay)
        
    elif choice == "3":
        producer.run_continuous(messages_per_second=500)
        
    elif choice == "4":
        producer.run_batch_mode(batch_size=100, batches=1, delay_between_batches=0)
        
    else:
        print("Lựa chọn không hợp lệ. Chạy test mode.")
        producer.run_batch_mode(batch_size=100, batches=1, delay_between_batches=0)

if __name__ == "__main__":
    main()