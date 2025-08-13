from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("KafkaDeliveryEventsHCM") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema
from pyspark.sql.types import StructType, StringType, DoubleType
schema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_name", StringType()) \
    .add("address", StringType()) \
    .add("phone", StringType()) \
    .add("delivery_status", StringType()) \
    .add("package_weight_kg", DoubleType()) \
    .add("delivery_date", StringType()) \
    .add("created_at", StringType()) \
    .add("region", StringType())

# Đọc từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers",
            "192.168.235.143:9092,192.168.235.144:9092,192.168.235.145:9092") \
    .option("subscribe", "delivery-events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Filter region = hcm
df_hcm = df_parsed.filter(col("region") == "hcm")

# In ra màn hình
query = df_hcm.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
