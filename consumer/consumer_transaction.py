from confluent_kafka import Consumer, KafkaError

def create_consumer():
    conf = {
        'bootstrap.servers': '192.168.235.136:9092,192.168.235.147:9092,192.168.235.148:9092',
        'group.id': 'transaction_consumer_group',
        'auto.offset.reset': 'earliest'  # đọc từ đầu nếu chưa có offset
    }
    consumer = Consumer(conf)
    consumer.subscribe(['transaction_data'])
    return consumer

def consume_messages():
    consumer = create_consumer()
    print("📥 Consumer started. Listening on topic 'transaction_data'...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Error: {msg.error()}")
                    break

            print(f"✅ Received: key={msg.key()} value={msg.value().decode('utf-8')} "
                  f"(partition={msg.partition()}, offset={msg.offset()})")

    except KeyboardInterrupt:
        print("🛑 Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages()
