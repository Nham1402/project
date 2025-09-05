from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv
import os
import logging
import time
import traceback

# ===============================
# Logging setup
# ===============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables (.env đặt ngoài project/consumer)
load_dotenv(dotenv_path=os.path.join("/home/hadoop/project", ".env"))

# ===============================
# Config
# ===============================
KAFKA_CONFIG = {
    'bootstrap.servers': '192.168.235.136:9092,192.168.235.147:9092,192.168.235.148:9092',
}

POSTGRES_CONFIG = {
    "url": f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
    "table": "banking_dw.fact_transaction",
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "driver": "org.postgresql.Driver"
}

logger.info(f"🔧 Database: {POSTGRES_CONFIG['url']}")
logger.info(f"🔧 Table: {POSTGRES_CONFIG['table']}")

# ===============================
# Schema khớp với bảng fact_transaction
# ===============================
TRANSACTION_SCHEMA = StructType([
    StructField("account_key", IntegerType(), True),
    StructField("customer_key", IntegerType(), True),
    StructField("location_key", IntegerType(), True),
    StructField("event_key", IntegerType(), True),
    StructField("application_key", IntegerType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("reference_number", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("transaction_category", StringType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("fee_amount", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("net_amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("account_number", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("description", StringType(), True),
    StructField("created_timestamp", TimestampType(), True),
    StructField("processed_timestamp", TimestampType(), True),
    StructField("updated_timestamp", TimestampType(), True)
])

# ===============================
# Realtime Write Function
# ===============================
def write_realtime_batch(batch_df, batch_id):
    start_time = time.time()
    
    try:
        row_count = batch_df.count()
        logger.info(f"⚡ Batch {batch_id}: Processing {row_count} records")
        
        if row_count == 0:
            return
        
        # Validate: transaction_id + transaction_amount phải có
        valid_df = batch_df.filter(
            col("transaction_id").isNotNull() &
            col("transaction_amount").isNotNull() &
            (col("transaction_amount") > 0)
        )
        
        valid_count = valid_df.count()
        if valid_count == 0:
            logger.warning(f"⚡ Batch {batch_id}: No valid records")
            return
        
        # Add processing timestamp
        final_df = valid_df.withColumn("realtime_processed_at", current_timestamp())
        
        # Write to PostgreSQL
        final_df.write \
            .format("jdbc") \
            .option("url", POSTGRES_CONFIG["url"]) \
            .option("dbtable", POSTGRES_CONFIG["table"]) \
            .option("user", POSTGRES_CONFIG["user"]) \
            .option("password", POSTGRES_CONFIG["password"]) \
            .option("driver", POSTGRES_CONFIG["driver"]) \
            .option("batchsize", "500") \
            .option("isolationLevel", "READ_UNCOMMITTED") \
            .option("numPartitions", "1") \
            .option("rewriteBatchedStatements", "true") \
            .mode("append") \
            .save()
        
        processing_time = time.time() - start_time
        logger.info(f"✅ Batch {batch_id}: Inserted {valid_count} records in {processing_time:.2f}s")
        
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"❌ Batch {batch_id} failed after {processing_time:.2f}s: {e}")
        logger.error(f"❌ Error details: {traceback.format_exc()}")

# ===============================
# Realtime Streaming Class
# ===============================
class RealtimeSparkStreaming:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("RealtimeKafkaToPostgres") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")
        logger.info("⚡ Spark session created for realtime processing")

    def create_realtime_stream(self):
        logger.info("⚡ Creating realtime Kafka stream...")
        
        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap.servers']) \
            .option("subscribe", "transaction_data") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "1000") \
            .option("kafka.consumer.cache.enabled", "false") \
            .load()

        parsed_df = kafka_df.select(
            col("key").cast("string"),
            from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("key", "data.*", "kafka_timestamp")

        parsed_df.printSchema()
        logger.info("✅ Realtime Kafka stream created successfully")
        return parsed_df

    def start_realtime_processing(self):
        df = self.create_realtime_stream()
        
        query = df.writeStream \
            .foreachBatch(write_realtime_batch) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/realtime_checkpoint") \
            .trigger(processingTime="2 seconds") \
            .start()

        logger.info("⚡ REALTIME PROCESSING STARTED!")
        query.awaitTermination()

# ===============================
# Main Function
# ===============================
def main():
    logger.info("🚀 STARTING REALTIME KAFKA TO POSTGRES STREAMING")
    realtime_stream = RealtimeSparkStreaming()
    realtime_stream.start_realtime_processing()

if __name__ == "__main__":
    main()
