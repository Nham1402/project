from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType  , DoubleType


spark = SparkSession.builder \
    .appName("KafkaDeliveryEventsHCM") \
    .master("spark://192.168.235.142:7077") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


#define the schema
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
    
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers",
            "192.168.235.143:9092,192.168.235.144:9092,192.168.235.145:9092") \
    .option("subscribe", "delivery_orders") \
    .option("startingOffsets", "latest") \
    .load()
    
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Lọc region = hcm
df_hcm = df_parsed.filter(col("region") == "hcm")

# Xuất ra console
query = df_hcm.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()