# producers/delivery_event_producer.py
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import json
import time
import threading
from datetime import datetime
from model.delivery_schemas import DeliveryDataGenerator
import random

class DeliveryEventProducer:
    def __init__(self, bootstrap_servers=None):
        if bootstrap_servers is None:
            bootstrap_servers = ['192.168.235.136:9092', '192.168.235.147:9092', '192.168.235.148:9092']
        self.bootstrap_servers = bootstrap_servers
        self.producer = Producer({'bootstrap.servers': ','.join(bootstrap_servers)})
        self.generator = DeliveryDataGenerator()
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

    def produce_order_events(self, topic='order_events', rate=50):
        print(f"🚚 Starting order events producer at {rate} events/second...")
        order_counter = 1
        customer_counter = 1

        def delivery_report(err, msg):
            if err:
                print(f"Delivery failed for {msg.key()}: {err}")

        while self.running:
            try:
                order_id = f"order_{order_counter:06d}"
                customer_id = f"customer_{customer_counter:06d}"
                event = self.generator.generate_order_event(order_id, customer_id)

                self.producer.produce(topic, key=order_id.encode('utf-8'), value=json.dumps(event).encode('utf-8'),
                                      callback=delivery_report)
                self.producer.poll(0)

                order_counter += 1
                if order_counter % 100 == 0:
                    customer_counter += 1

                time.sleep(1/rate)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error producing order event: {e}")
                time.sleep(1)

    def produce_gps_tracking(self, topic='gps_tracking', rate=100):
        print(f"📍 Starting GPS tracking producer at {rate} events/second...")
        drivers = [f"driver_{i:03d}" for i in range(1, 51)]
        vehicles = [f"vehicle_{i:03d}" for i in range(1, 51)]

        def delivery_report(err, msg):
            if err:
                print(f"Delivery failed for {msg.key()}: {err}")

        while self.running:
            try:
                for i in range(min(rate//10, len(drivers))):
                    driver_id = random.choice(drivers)
                    vehicle_id = random.choice(vehicles)
                    tracking_data = self.generator.generate_gps_tracking(driver_id, vehicle_id)
                    self.producer.produce(topic, key=driver_id.encode('utf-8'), value=json.dumps(tracking_data).encode('utf-8'),
                                          callback=delivery_report)
                    self.producer.poll(0)

                time.sleep(10)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error producing GPS tracking: {e}")
                time.sleep(1)

    def produce_customer_behavior(self, topic='customer_behavior', rate=30):
        print(f"👤 Starting customer behavior producer at {rate} events/second...")
        event_types = ['app_open', 'search', 'order_create', 'track_order', 'rate_delivery']
        devices = ['mobile', 'web']
        os_types = ['iOS', 'Android', 'Web']

        def delivery_report(err, msg):
            if err:
                print(f"Delivery failed for {msg.key()}: {err}")

        while self.running:
            try:
                customer_id = f"customer_{random.randint(1, 1000):06d}"
                event = {
                    "customer_id": customer_id,
                    "session_id": f"session_{random.randint(1, 10000)}",
                    "event_type": random.choice(event_types),
                    "timestamp": datetime.now().isoformat(),
                    "device_info": {
                        "device_type": random.choice(devices),
                        "os": random.choice(os_types),
                        "app_version": f"v{random.randint(1,5)}.{random.randint(0,9)}"
                    },
                    "location": {
                        "lat": 21.0285 + random.uniform(-0.1, 0.1),
                        "lon": 105.8542 + random.uniform(-0.1, 0.1),
                        "city": "Hà Nội"
                    },
                    "event_data": json.dumps({
                        "screen": random.choice(["home", "search", "order", "tracking", "profile"]),
                        "action": random.choice(["click", "scroll", "search", "submit"])
                    })
                }

                self.producer.produce(topic, key=customer_id.encode('utf-8'), value=json.dumps(event).encode('utf-8'),
                                      callback=delivery_report)
                self.producer.poll(0)
                time.sleep(1/rate)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error producing customer behavior: {e}")
                time.sleep(1)

    def start_all_producers(self):
        self.create_topic_if_not_exists('order_events')
        self.create_topic_if_not_exists('gps_tracking')
        self.create_topic_if_not_exists('customer_behavior')

        threads = [
            threading.Thread(target=self.produce_order_events, kwargs={'rate': 50}),
            threading.Thread(target=self.produce_gps_tracking, kwargs={'rate': 100}),
            threading.Thread(target=self.produce_customer_behavior, kwargs={'rate': 30})
        ]

        for thread in threads:
            thread.daemon = True
            thread.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.running = False
            print("🛑 Stopping all producers...")

if __name__ == "__main__":
    producer = DeliveryEventProducer()
    producer.start_all_producers()
