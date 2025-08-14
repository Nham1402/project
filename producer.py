from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
import threading
import signal
import sys
from datetime import datetime, timedelta

fake = Faker(['vi_VN', 'en_US'])  # Thêm locale Việt Nam

class DeliveryDataProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['192.168.235.143:9092', '192.168.235.144:9092', '192.168.235.145:9092'],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            # Tối ưu cho throughput cao
            batch_size=16384,  # 16KB batch
            linger_ms=10,      # Đợi 10ms để tích lũy messages
            compression_type='snappy',
            acks='1',          # Chờ leader acknowledge
            retries=3,
            max_in_flight_requests_per_connection=5,
            buffer_memory=33554432  # 32MB buffer
        )
        
        self.topic_name = "delivery_orders"
        self.running = True
        self.sent_count = 0
        self.error_count = 0
        
        # Setup signal handler để graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        print(f"\n🛑 Nhận signal {signum}. Đang dừng producer...")
        self.running = False
        
    def fake_delivery_data(self):
        """Tạo dữ liệu fake với đa dạng hơn"""
        # Tạo timestamp trong khoảng gần đây để test partitioning
        now = datetime.now()
        random_days_ago = random.randint(0, 7)  # 7 ngày gần đây
        random_hours_ago = random.randint(0, 23)
        delivery_date = now - timedelta(days=random_days_ago, hours=random_hours_ago)
        created_date = delivery_date - timedelta(hours=random.randint(1, 48))
        
        # Địa chỉ Việt Nam realistic hơn
        vietnamese_addresses = [
            "123 Nguyễn Huệ, Quận 1, TP.HCM",
            "456 Lê Lợi, Quận Hai Bà Trưng, Hà Nội", 
            "789 Trần Phú, Quận Hải Châu, Đà Nẵng",
            "321 Võ Văn Tần, Quận 3, TP.HCM",
            "654 Hoàng Diệu, Quận 4, TP.HCM",
            "987 Lý Thường Kiệt, Quận 10, TP.HCM"
        ]
        
        vietnamese_names = [
            "Nguyễn Văn An", "Trần Thị Bình", "Lê Hoàng Cường",
            "Phạm Thu Dung", "Hoàng Minh Đức", "Vũ Thị Hương",
            "Đặng Quốc Khánh", "Bùi Thị Lan", "Dương Văn Minh"
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
            # Thêm một số fields bổ sung
            "delivery_fee": round(random.uniform(15000, 50000), 0),
            "package_value": round(random.uniform(100000, 2000000), 0),
            "delivery_type": random.choice(["standard", "express", "same_day"])
        }

    def send_callback(self, record_metadata=None, exception=None):
        """Callback khi gửi message"""
        if exception is not None:
            self.error_count += 1
            print(f"❌ Lỗi gửi message: {exception}")
        else:
            self.sent_count += 1
            if self.sent_count % 1000 == 0:
                print(f"✅ Đã gửi {self.sent_count} messages thành công")

    def produce_batch(self, batch_size=100):
        """Tạo và gửi một batch messages"""
        for _ in range(batch_size):
            if not self.running:
                break
                
            data = self.fake_delivery_data()
            
            # Sử dụng region làm partition key để đảm bảo data cùng region vào cùng partition
            partition_key = data['region']
            
            try:
                future = self.producer.send(
                    self.topic_name, 
                    value=data, 
                    key=partition_key
                )
                future.add_callback(self.send_callback)
                
            except Exception as e:
                print(f"❌ Lỗi khi gửi message: {e}")
                self.error_count += 1

    def run_continuous(self, messages_per_second=50):
        """Chạy producer liên tục với tốc độ được chỉ định"""
        print(f"🚀 Bắt đầu gửi messages với tốc độ {messages_per_second} msg/s")
        print(f"📡 Topic: {self.topic_name}")
        print("🔄 Nhấn Ctrl+C để dừng\n")
        
        batch_size = min(messages_per_second, 100)  # Max 100 per batch
        sleep_time = batch_size / messages_per_second
        
        last_stats_time = time.time()
        
        try:
            while self.running:
                batch_start = time.time()
                
                # Gửi batch
                self.produce_batch(batch_size)
                
                # Flush để đảm bảo messages được gửi
                self.producer.flush(timeout=10)
                
                # In thống kê mỗi 30 giây
                if time.time() - last_stats_time >= 30:
                    self.print_stats()
                    last_stats_time = time.time()
                
                # Sleep để maintain tốc độ
                elapsed = time.time() - batch_start
                if elapsed < sleep_time:
                    time.sleep(sleep_time - elapsed)
                    
        except KeyboardInterrupt:
            print("\n🛑 Nhận Ctrl+C. Đang dừng producer...")
        finally:
            self.cleanup()

    def run_burst_mode(self, total_messages=10000, burst_size=500):
        """Chế độ gửi burst để test"""
        print(f"💥 Chế độ burst: {total_messages} messages, {burst_size} per burst")
        
        sent = 0
        while sent < total_messages and self.running:
            batch_size = min(burst_size, total_messages - sent)
            
            print(f"📤 Gửi burst {sent + 1} - {sent + batch_size}")
            self.produce_batch(batch_size)
            self.producer.flush(timeout=30)
            
            sent += batch_size
            
            if sent < total_messages:
                time.sleep(2)  # Pause giữa các burst
        
        self.print_stats()
        self.cleanup()

    def print_stats(self):
        """In thống kê"""
        print(f"📊 Thống kê: ✅ {self.sent_count} sent, ❌ {self.error_count} errors")

    def cleanup(self):
        """Cleanup resources"""
        print("🧹 Đang cleanup...")
        self.producer.flush(timeout=30)
        self.producer.close(timeout=30)
        self.print_stats()
        print("✅ Producer đã dừng hoàn toàn")

def main():
    producer = DeliveryDataProducer()
    
    print("Chọn chế độ chạy:")
    print("1. Continuous (liên tục với tốc độ ổn định)")
    print("2. Burst (gửi theo đợt để test)")
    print("3. High throughput (tốc độ cao)")
    
    choice = input("Nhập lựa chọn (1-3): ").strip()
    
    if choice == "1":
        rate = int(input("Nhập số messages/second (mặc định 50): ") or "50")
        producer.run_continuous(messages_per_second=rate)
    elif choice == "2":
        total = int(input("Tổng số messages (mặc định 10000): ") or "10000")
        burst = int(input("Messages per burst (mặc định 500): ") or "500")
        producer.run_burst_mode(total_messages=total, burst_size=burst)
    elif choice == "3":
        producer.run_continuous(messages_per_second=500)  # High throughput
    else:
        print("Lựa chọn không hợp lệ. Chạy chế độ mặc định.")
        producer.run_continuous()

if __name__ == "__main__":
    main()