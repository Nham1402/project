from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

# ---------------------------
# Tạo SparkSession
# ---------------------------
spark = SparkSession.builder \
    .appName("KafkaDeliveryEventsHCM") \
    .master("spark://192.168.235.142:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .config("spark.sql.shuffle.partitions", "12") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------
# Tắt compaction tạm thời để tránh lỗi _spark_metadata
# ---------------------------
spark.conf.set("spark.sql.streaming.fileSource.log.compactInterval", "0")

# ---------------------------
# Schema dữ liệu
# ---------------------------
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

# ---------------------------
# Đọc stream từ Kafka
# ---------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.235.143:9092,192.168.235.144:9092,192.168.235.145:9092") \
    .option("subscribe", "delivery_orders") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 5000) \
    .load()

# ---------------------------
# Parse JSON
# ---------------------------
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema, {"mode": "PERMISSIVE"}).alias("data")) \
    .select("data.*")

# ---------------------------
# Gom nhiều bản ghi vào 1 partition để giảm small files
# ---------------------------
df_parsed_repart = df_parsed.repartition(1)

# ---------------------------
# Ghi ra HDFS Parquet + checkpoint riêng
# ---------------------------
query = df_parsed_repart.writeStream \
    .format("parquet") \
    .option("path", "hdfs://192.168.235.142:9000/user/hadoop/input/delivery-events/") \
    .option("checkpointLocation", "hdfs://192.168.235.142:9000/user/hadoop/checkpoints/delivery-events/") \
    .option("maxRecordsPerFile", 10000) \
    .outputMode("append") \
    .start()

query.awaitTermination()
