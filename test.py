from kafka import kafkaProducer, KafkaConsumer
from faker import Faker
import json
import random
import time

fake =  Faker()

producer = KafkaProducer(
    bootstrap_servers=['master-node:9092'],  # thay bằng IP hoặc hostname Kafka broker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fake_delivery_data():
    return {
        "order_id": fake.uuid4(),
        "customer_name": fake.name(),
        "address": fake.address(),
        "phone": fake.phone_number(),
        "delivery_status": random.choice(["pending", "shipped", "delivered", "canceled"]),
        "package_weight_kg": round(random.uniform(0.5, 20.0), 2),
        "delivery_date": fake.date_time_this_year().isoformat(),
        "created_at": fake.date_time_this_year().isoformat()
    }
if __name__ == "__main__":
    topic_name = "delivery_orders"
    while True:
        data = fake_delivery_data()
        producer.send(topic_name, value=data)
        print(f"Sent: {data}")
        time.sleep(1)
