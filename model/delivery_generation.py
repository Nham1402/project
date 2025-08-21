# models/delivery_schemas.py
#from pyspark.sql.types import *
from datetime import datetime
from typing import Optional
import json

# class DeliveryDataSchemas:
    
#     # Order Events Schema
#     ORDER_EVENT_SCHEMA = StructType([
#         StructField("order_id", StringType(), False),
#         StructField("customer_id", StringType(), False),
#         StructField("driver_id", StringType(), True),
#         StructField("event_type", StringType(), False),  # created, assigned, picked_up, in_transit, delivered, cancelled
#         StructField("timestamp", TimestampType(), False),
#         StructField("location", StructType([
#             StructField("lat", DoubleType(), False),
#             StructField("lon", DoubleType(), False),
#             StructField("address", StringType(), True)
#         ]), True),
#         StructField("order_details", StructType([
#             StructField("items", ArrayType(StructType([
#                 StructField("product_id", StringType()),
#                 StructField("quantity", IntegerType()),
#                 StructField("weight", DoubleType()),
#                 StructField("category", StringType())
#             ])), True),
#             StructField("total_value", DoubleType(), True),
#             StructField("total_weight", DoubleType(), True),
#             StructField("delivery_fee", DoubleType(), True),
#             StructField("priority", StringType(), True),  # standard, express, same_day
#             StructField("special_requirements", ArrayType(StringType()), True)
#         ]), True),
#         StructField("pickup_location", StructType([
#             StructField("lat", DoubleType()),
#             StructField("lon", DoubleType()),
#             StructField("address", StringType()),
#             StructField("contact_phone", StringType())
#         ]), True),
#         StructField("delivery_location", StructType([
#             StructField("lat", DoubleType()),
#             StructField("lon", DoubleType()),
#             StructField("address", StringType()),
#             StructField("contact_phone", StringType())
#         ]), True)
#     ])
    
#     # GPS Tracking Schema
#     GPS_TRACKING_SCHEMA = StructType([
#         StructField("driver_id", StringType(), False),
#         StructField("vehicle_id", StringType(), False),
#         StructField("timestamp", TimestampType(), False),
#         StructField("location", StructType([
#             StructField("lat", DoubleType(), False),
#             StructField("lon", DoubleType(), False),
#             StructField("accuracy", DoubleType(), True),
#             StructField("speed", DoubleType(), True),  # km/h
#             StructField("heading", DoubleType(), True)  # degrees
#         ]), False),
#         StructField("vehicle_status", StructType([
#             StructField("engine_status", StringType(), True),  # on, off
#             StructField("fuel_level", DoubleType(), True),
#             StructField("temperature", DoubleType(), True),
#             StructField("load_weight", DoubleType(), True)
#         ]), True),
#         StructField("driver_status", StringType(), True),  # available, busy, break, offline
#         StructField("current_orders", ArrayType(StringType()), True)
#     ])
    
#     # Driver Performance Schema
#     DRIVER_PERFORMANCE_SCHEMA = StructType([
#         StructField("driver_id", StringType(), False),
#         StructField("date", DateType(), False),
#         StructField("shift_start", TimestampType(), True),
#         StructField("shift_end", TimestampType(), True),
#         StructField("total_deliveries", IntegerType(), True),
#         StructField("successful_deliveries", IntegerType(), True),
#         StructField("failed_deliveries", IntegerType(), True),
#         StructField("total_distance", DoubleType(), True),  # km
#         StructField("total_revenue", DoubleType(), True),
#         StructField("avg_delivery_time", DoubleType(), True),  # minutes
#         StructField("customer_ratings", ArrayType(DoubleType()), True),
#         StructField("avg_rating", DoubleType(), True),
#         StructField("fuel_consumption", DoubleType(), True),  # liters
#         StructField("violations", ArrayType(StructType([
#             StructField("type", StringType()),
#             StructField("timestamp", TimestampType()),
#             StructField("severity", StringType())
#         ])), True)
#     ])

#     # Customer Behavior Schema
#     CUSTOMER_BEHAVIOR_SCHEMA = StructType([
#         StructField("customer_id", StringType(), False),
#         StructField("session_id", StringType(), True),
#         StructField("event_type", StringType(), False),  # app_open, search, order_create, track_order, rate_delivery
#         StructField("timestamp", TimestampType(), False),
#         StructField("device_info", StructType([
#             StructField("device_type", StringType()),  # mobile, web
#             StructField("os", StringType()),
#             StructField("app_version", StringType())
#         ]), True),
#         StructField("location", StructType([
#             StructField("lat", DoubleType()),
#             StructField("lon", DoubleType()),
#             StructField("city", StringType())
#         ]), True),
#         StructField("event_data", StringType(), True)  # JSON string with event-specific data
#     ])

# Sample data generators for testing
class DeliveryDataGenerator:
    def __init__(self):
        self.order_statuses = ['created', 'assigned', 'picked_up', 'in_transit', 'delivered', 'cancelled']
        self.priorities = ['standard', 'express', 'same_day']
        self.driver_statuses = ['available', 'busy', 'break', 'offline']
        
    def generate_order_event(self, order_id: str, customer_id: str) -> dict:
        """Generate sample order event"""
        import random
        from datetime import datetime, timedelta
        
        return {
            "order_id": order_id,
            "customer_id": customer_id,
            "driver_id": f"driver_{random.randint(1, 100)}",
            "event_type": random.choice(self.order_statuses),
            "timestamp": datetime.now().isoformat(),
            "location": {
                "lat": 21.0285 + random.uniform(-0.1, 0.1),  # Hanoi coordinates
                "lon": 105.8542 + random.uniform(-0.1, 0.1),
                "address": f"Địa chỉ {random.randint(1, 1000)}, Hà Nội"
            },
            "order_details": {
                "items": [
                    {
                        "product_id": f"prod_{random.randint(1, 1000)}",
                        "quantity": random.randint(1, 5),
                        "weight": random.uniform(0.5, 5.0),
                        "category": random.choice(["food", "electronics", "clothing", "books"])
                    }
                ],
                "total_value": random.uniform(50000, 500000),
                "total_weight": random.uniform(0.5, 10.0),
                "delivery_fee": random.uniform(15000, 50000),
                "priority": random.choice(self.priorities)
            },
            "pickup_location": {
                "lat": 21.0285 + random.uniform(-0.05, 0.05),
                "lon": 105.8542 + random.uniform(-0.05, 0.05),
                "address": f"Cửa hàng {random.randint(1, 100)}, Hà Nội",
                "contact_phone": f"0{random.randint(900000000, 999999999)}"
            },
            "delivery_location": {
                "lat": 21.0285 + random.uniform(-0.1, 0.1),
                "lon": 105.8542 + random.uniform(-0.1, 0.1),
                "address": f"Nhà {random.randint(1, 500)}, Hà Nội",
                "contact_phone": f"0{random.randint(900000000, 999999999)}"
            }
        }
    
    def generate_gps_tracking(self, driver_id: str, vehicle_id: str) -> dict:
        """Generate sample GPS tracking data"""
        import random
        from datetime import datetime
        
        return {
            "driver_id": driver_id,
            "vehicle_id": vehicle_id,
            "timestamp": datetime.now().isoformat(),
            "location": {
                "lat": 21.0285 + random.uniform(-0.2, 0.2),
                "lon": 105.8542 + random.uniform(-0.2, 0.2),
                "accuracy": random.uniform(5, 20),
                "speed": random.uniform(0, 60),
                "heading": random.uniform(0, 360)
            },
            "vehicle_status": {
                "engine_status": random.choice(["on", "off"]),
                "fuel_level": random.uniform(10, 100),
                "temperature": random.uniform(15, 35),
                "load_weight": random.uniform(0, 500)
            },
            "driver_status": random.choice(self.driver_statuses),
            "current_orders": [f"order_{random.randint(1, 1000)}" for _ in range(random.randint(0, 3))]
        }