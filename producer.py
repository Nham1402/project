from kafka import KafkaProducer
import json
import time

def test_minimal_producer():
    """Producer tối giản để test kết nối"""
    
    print("🧪 Testing minimal Kafka producer...")
    
    try:
        # Tạo producer đơn giản nhất
        producer = KafkaProducer(
            bootstrap_servers=['192.168.235.143:9092'],  # Chỉ dùng 1 broker
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=10000,    # 10 seconds
            acks='1',
            retries=1
        )
        
        print("✅ Producer created successfully")
        
        # Gửi 1 message test
        test_data = {"test": "hello kafka", "timestamp": time.time()}
        
        future = producer.send('delivery_orders', test_data)
        result = future.get(timeout=10)
        
        print(f"✅ Message sent successfully: {result}")
        
        # Đóng producer
        producer.close()
        print("✅ Producer closed successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_with_console():
    """Test bằng console producer"""
    print("\n💡 Nếu Python producer không hoạt động, thử console producer:")
    print("kafka-console-producer.sh --bootstrap-server 192.168.235.143:9092 --topic delivery_orders")
    print("Sau đó gõ vài dòng text và nhấn Enter")

if __name__ == "__main__":
    success = test_minimal_producer()
    
    if not success:
        test_with_console()
        print("\n🔧 Các bước troubleshoot:")
        print("1. Kiểm tra Kafka có đang chạy: jps | grep Kafka")
        print("2. Kiểm tra ZooKeeper: jps | grep QuorumPeerMain") 
        print("3. Kiểm tra network: telnet 192.168.235.143 9092")
        print("4. Xem Kafka logs: tail -f $KAFKA_HOME/logs/server.log")
    else:
        print("🎉 Kafka connection OK! Có thể chạy producer chính.")