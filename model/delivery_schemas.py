from pyspark.sql.types import *
from datetime import datetime
from typing import Optional
import json

class DeliveryDataSchemas:
    
    # Order Events Schema
    ORDER_EVENT_SCHEMA = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("driver_id", StringType(), True),
        StructField("event_type", StringType(), False),  # created, assigned, picked_up, in_transit, delivered, cancelled
        StructField("timestamp", TimestampType(), False),
        StructField("location", StructType([
            StructField("lat", DoubleType(), False),
            StructField("lon", DoubleType(), False),
            StructField("address", StringType(), True)
        ]), True),
        StructField("order_details", StructType([
            StructField("items", ArrayType(StructType([
                StructField("product_id", StringType()),
                StructField("quantity", IntegerType()),
                StructField("weight", DoubleType()),
                StructField("category", StringType())
            ])), True),
            StructField("total_value", DoubleType(), True),
            StructField("total_weight", DoubleType(), True),
            StructField("delivery_fee", DoubleType(), True),
            StructField("priority", StringType(), True),  # standard, express, same_day
            StructField("special_requirements", ArrayType(StringType()), True)
        ]), True),
        StructField("pickup_location", StructType([
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType()),
            StructField("address", StringType()),
            StructField("contact_phone", StringType())
        ]), True),
        StructField("delivery_location", StructType([
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType()),
            StructField("address", StringType()),
            StructField("contact_phone", StringType())
        ]), True)
    ])
    
    # GPS Tracking Schema
    GPS_TRACKING_SCHEMA = StructType([
        StructField("driver_id", StringType(), False),
        StructField("vehicle_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("location", StructType([
            StructField("lat", DoubleType(), False),
            StructField("lon", DoubleType(), False),
            StructField("accuracy", DoubleType(), True),
            StructField("speed", DoubleType(), True),  # km/h
            StructField("heading", DoubleType(), True)  # degrees
        ]), False),
        StructField("vehicle_status", StructType([
            StructField("engine_status", StringType(), True),  # on, off
            StructField("fuel_level", DoubleType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("load_weight", DoubleType(), True)
        ]), True),
        StructField("driver_status", StringType(), True),  # available, busy, break, offline
        StructField("current_orders", ArrayType(StringType()), True)
    ])
    
    # Driver Performance Schema
    DRIVER_PERFORMANCE_SCHEMA = StructType([
        StructField("driver_id", StringType(), False),
        StructField("date", DateType(), False),
        StructField("shift_start", TimestampType(), True),
        StructField("shift_end", TimestampType(), True),
        StructField("total_deliveries", IntegerType(), True),
        StructField("successful_deliveries", IntegerType(), True),
        StructField("failed_deliveries", IntegerType(), True),
        StructField("total_distance", DoubleType(), True),  # km
        StructField("total_revenue", DoubleType(), True),
        StructField("avg_delivery_time", DoubleType(), True),  # minutes
        StructField("customer_ratings", ArrayType(DoubleType()), True),
        StructField("avg_rating", DoubleType(), True),
        StructField("fuel_consumption", DoubleType(), True),  # liters
        StructField("violations", ArrayType(StructType([
            StructField("type", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("severity", StringType())
        ])), True)
    ])

    # Customer Behavior Schema
    CUSTOMER_BEHAVIOR_SCHEMA = StructType([
        StructField("customer_id", StringType(), False),
        StructField("session_id", StringType(), True),
        StructField("event_type", StringType(), False),  # app_open, search, order_create, track_order, rate_delivery
        StructField("timestamp", TimestampType(), False),
        StructField("device_info", StructType([
            StructField("device_type", StringType()),  # mobile, web
            StructField("os", StringType()),
            StructField("app_version", StringType())
        ]), True),
        StructField("location", StructType([
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType()),
            StructField("city", StringType())
        ]), True),
        StructField("event_data", StringType(), True)  # JSON string with event-specific data
    ])