from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# ---------------------------
# Cấu hình logging
# ---------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------
# Tạo SparkSession với cấu hình tối ưu
# ---------------------------
spark = SparkSession.builder \
    .appName("KafkaDeliveryEventsHCM_Optimized") \
    .master("spark://192.168.235.142:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.sql.streaming.minBatchesToRetain", "10") \
    .config("spark.sql.streaming.fileSource.log.compactInterval", "10") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.streaming.checkpointFileManagerClass", "org.apache.spark.sql.execution.streaming.CheckpointFileManager") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------
# Schema dữ liệu với validation
# ---------------------------
schema = StructType() \
    .add("order_id", StringType(), True) \
    .add("customer_name", StringType(), True) \
    .add("address", StringType(), True) \
    .add("phone", StringType(), True) \
    .add("delivery_status", StringType(), True) \
    .add("package_weight_kg", DoubleType(), True) \
    .add("delivery_date", StringType(), True) \
    .add("created_at", StringType(), True) \
    .add("region", StringType(), True)

print("🚀 Bắt đầu Spark Streaming job...")

# ---------------------------
# Đọc stream từ Kafka với cấu hình tối ưu
# ---------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.235.143:9092,192.168.235.144:9092,192.168.235.145:9092") \
    .option("subscribe", "delivery_orders") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "10000") \
    .option("kafka.consumer.group.id", "spark_streaming_delivery_group") \
    .option("kafka.session.timeout.ms", "30000") \
    .option("kafka.request.timeout.ms", "40000") \
    .load()

# ---------------------------
# Parse JSON với error handling
# ---------------------------
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str", "timestamp", "offset") \
    .select(
        from_json(col("json_str"), schema, {"mode": "PERMISSIVE"}).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("offset").alias("kafka_offset")
    ) \
    .select("data.*", "kafka_timestamp", "kafka_offset") \
    .filter(col("order_id").isNotNull())  # Lọc bỏ records lỗi

# ---------------------------
# Thêm cột partitioning để tối ưu storage
# ---------------------------
df_with_partitions = df_parsed \
    .withColumn("delivery_date_parsed", 
                when(col("delivery_date").isNotNull(), 
                     to_timestamp(col("delivery_date")))
                .otherwise(current_timestamp())) \
    .withColumn("year", year(col("delivery_date_parsed"))) \
    .withColumn("month", month(col("delivery_date_parsed"))) \
    .withColumn("day", dayofmonth(col("delivery_date_parsed"))) \
    .withColumn("processing_time", current_timestamp()) \
    .drop("delivery_date_parsed")

# ---------------------------
# Tối ưu partitioning - không dùng repartition(1) vì sẽ tạo bottleneck
# Thay vào đó sử dụng coalesce và partitioning thông minh
# ---------------------------
df_optimized = df_with_partitions.coalesce(2)  # Giảm số partitions nhưng không về 1

print("📊 Schema của DataFrame:")
df_optimized.printSchema()

# ---------------------------
# Ghi ra HDFS với partitioning và batch control
# ---------------------------
def write_batch(batch_df, batch_id):
    """
    Custom function để ghi từng batch với control tốt hơn
    """
    try:
        print(f"🔄 Processing batch {batch_id} với {batch_df.count()} records...")
        
        # Ghi với partitioning theo date và region
        batch_df.write \
            .mode("append") \
            .partitionBy("year", "month", "day", "region") \
            .option("maxRecordsPerFile", "20000") \
            .parquet("hdfs://192.168.235.142:9000/user/hadoop/input/delivery-events/")
            
        print(f"✅ Hoàn thành batch {batch_id}")
        
    except Exception as e:
        print(f"❌ Lỗi khi ghi batch {batch_id}: {str(e)}")
        raise e

# ---------------------------
# Streaming query với trigger interval và watermarking
# ---------------------------
query = df_optimized.writeStream \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "hdfs://192.168.235.142:9000/user/hadoop/checkpoints/delivery-events/") \
    .trigger(processingTime="2 minutes") \
    .start()

# ---------------------------
# Alternative: Ghi trực tiếp với file-based sink (nếu không dùng foreachBatch)
# ---------------------------
"""
query_alternative = df_optimized.writeStream \
    .format("parquet") \
    .option("path", "hdfs://192.168.235.142:9000/user/hadoop/input/delivery-events/") \
    .option("checkpointLocation", "hdfs://192.168.235.142:9000/user/hadoop/checkpoints/delivery-events/") \
    .partitionBy("year", "month", "day", "region") \
    .option("maxRecordsPerFile", "20000") \
    .trigger(processingTime="2 minutes") \
    .outputMode("append") \
    .start()
"""

# ---------------------------
# Monitoring và cleanup
# ---------------------------
def monitor_streaming():
    """Monitoring function"""
    try:
        while query.isActive:
            progress = query.lastProgress
            if progress:
                print(f"📈 Batch: {progress.get('batchId', 'N/A')}, "
                      f"Input rows: {progress.get('inputRowsPerSecond', 'N/A')}, "
                      f"Processing time: {progress.get('durationMs', {}).get('triggerExecution', 'N/A')}ms")
            
            query.awaitTermination(60)  # Check every minute
            
    except KeyboardInterrupt:
        print("🛑 Stopping streaming job...")
        query.stop()
        spark.stop()
    except Exception as e:
        print(f"❌ Streaming error: {str(e)}")
        query.stop()
        spark.stop()
        raise e

# ---------------------------
# Chạy monitoring
# ---------------------------
if __name__ == "__main__":
    try:
        print("🎯 Streaming job đang chạy. Nhấn Ctrl+C để dừng.")
        monitor_streaming()
    finally:
        print("🏁 Cleaning up resources...")
        if 'query' in locals() and query.isActive:
            query.stop()
        spark.stop()
        print("✅ Đã dừng thành công!")