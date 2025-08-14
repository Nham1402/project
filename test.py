from kafka import KafkaProducer
from faker import Faker
import json
import random
import time

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=['192.168.235.143:9092', '192.168.235.144:9092', '192.168.235.145:9092'],
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
        "created_at": fake.date_time_this_year().isoformat(),
        "region": random.choice(["hcm", "hn", "dn"]) 
    }

if __name__ == "__main__":
    topic_name = "delivery_orders"
    while True:
        for _ in range(100):
            data = fake_delivery_data()
            producer.send(topic_name, value=data)
            print(f"Sent: {data}")
        time.sleep(0.5)
