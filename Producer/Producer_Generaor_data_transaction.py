from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv
import os
import logging

# ===============================
# Logging setup
# ===============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(dotenv_path=os.path.join("/home/hadoop/project", ".env"))

# ===============================
# Config for Realtime
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

TRANSACTION_SCHEMA = StructType([
    StructField("account_key", IntegerType(), True),
    StructField("customer_key", IntegerType(), True),
    StructField("location_key", IntegerType(), True),
    StructField("event_key", LongType(), True),
    StructField("application_key", IntegerType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("reference_number", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("transaction_category", StringType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("fee_amount", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("net_amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("account_number", StringType(), True),
    StructField("transaction_status", StringType(), True),
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
    """
    Realtime batch processing - xử lý ngay khi có data
    """
    start_time = time.time()
    
    try:
        row_count = batch_df.count()
        logger.info(f"⚡ Batch {batch_id}: Processing {row_count} records")
        
        if row_count == 0:
            logger.info(f"⚡ Batch {batch_id}: Empty batch - skipping")
            return
        
        # Quick validation - chỉ check những field quan trọng nhất
        valid_df = batch_df.filter(
            col("transaction_id").isNotNull() &
            col("transaction_amount").isNotNull()
        )
        
        valid_count = valid_df.count()
        
        if valid_count == 0:
            logger.warning(f"⚡ Batch {batch_id}: No valid records")
            return
        
        # Add processing timestamp
        final_df = valid_df.withColumn("realtime_processed_at", current_timestamp())
        
        # Log sample data (only first record for speed)
        if logger.isEnabledFor(logging.DEBUG):
            sample = final_df.limit(1).collect()[0]
            logger.debug(f"⚡ Sample: {sample['transaction_id']} - ${sample['transaction_amount']}")
        
        # Write to PostgreSQL with optimized settings
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
        # Log lỗi nhưng không crash stream
        import traceback
        logger.error(f"❌ Error details: {traceback.format_exc()}")

# ===============================
# Realtime Streaming Class
# ===============================
class RealtimeSparkStreaming:
    def __init__(self):
        # Optimized Spark config for realtime
        self.spark = SparkSession.builder \
            .appName("RealtimeKafkaToPostgres") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")  # Reduce noise
        logger.info("⚡ Spark session created for realtime processing")

    def create_realtime_stream(self):
        """
        Tạo stream optimized cho realtime processing
        """
        try:
            logger.info("⚡ Creating realtime Kafka stream...")
            
            # Read from Kafka with realtime settings
            kafka_df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap.servers']) \
                .option("subscribe", "transaction_data") \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .option("maxOffsetsPerTrigger", "1000") \
                .option("kafka.consumer.cache.enabled", "false") \
                .load()

            # Parse JSON immediately
            parsed_df = kafka_df.select(
                col("key").cast("string"),
                from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ).select("key", "data.*", "kafka_timestamp")

            logger.info("✅ Realtime Kafka stream created successfully")
            return parsed_df

        except Exception as e:
            logger.error(f"❌ Failed to create Kafka stream: {e}")
            raise

    def start_realtime_processing(self):
        """
        Bắt đầu realtime processing
        """
        try:
            # Create stream
            df = self.create_realtime_stream()
            
            # Start realtime processing with minimal trigger interval
            query = df.writeStream \
                .foreachBatch(write_realtime_batch) \
                .outputMode("append") \
                .option("checkpointLocation", "/tmp/realtime_checkpoint") \
                .trigger(processingTime="2 seconds") \
                .start()

            logger.info("⚡ REALTIME PROCESSING STARTED!")
            logger.info("⚡ Processing every 2 seconds for minimal latency")
            logger.info("⚡ Press Ctrl+C to stop...")

            # Monitor processing
            import time
            last_progress_time = 0
            
            while query.isActive:
                time.sleep(5)  # Check every 5 seconds
                
                progress = query.lastProgress
                if progress:
                    current_time = time.time()
                    if current_time - last_progress_time > 30:  # Log every 30 seconds
                        input_rate = progress.get('inputRowsPerSecond', 0)
                        processing_rate = progress.get('processedRowsPerSecond', 0)
                        batch_duration = progress.get('durationMs', {}).get('triggerExecution', 0)
                        
                        logger.info(f"⚡ REALTIME STATS:")
                        logger.info(f"   📊 Input Rate: {input_rate:.1f} records/sec")
                        logger.info(f"   📊 Processing Rate: {processing_rate:.1f} records/sec")
                        logger.info(f"   📊 Batch Duration: {batch_duration}ms")
                        
                        last_progress_time = current_time

            query.awaitTermination()

        except KeyboardInterrupt:
            logger.info("⚡ Stopping realtime processing...")
            query.stop()
        except Exception as e:
            logger.error(f"❌ Realtime processing error: {e}")
            raise

# ===============================
# Quick Health Check
# ===============================
def quick_health_check():
    """
    Quick health check trước khi start realtime processing
    """
    try:
        logger.info("🏥 Running quick health check...")
        
        # Check environment variables
        required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASS']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            logger.error(f"❌ Missing environment variables: {missing_vars}")
            return False
        
        # Quick Spark test
        spark_test = SparkSession.builder.appName("HealthCheck").getOrCreate()
        test_df = spark_test.sql("SELECT 1 as health_check")
        assert test_df.count() == 1
        
        logger.info("✅ Health check passed - ready for realtime processing!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
        return False

# ===============================
# Main Function
# ===============================
import time

def main():
    """
    Main realtime processing
    """
    try:
        logger.info("🚀 STARTING REALTIME KAFKA TO POSTGRES STREAMING")
        logger.info("=" * 60)
        
        # Quick health check
        if not quick_health_check():
            logger.error("❌ Health check failed - aborting")
            return
        
        # Initialize realtime streaming
        realtime_stream = RealtimeSparkStreaming()
        
        # Start processing
        realtime_stream.start_realtime_processing()
        
    except KeyboardInterrupt:
        logger.info("🛑 Realtime processing stopped by user")
    except Exception as e:
        logger.error(f"❌ Fatal error in main: {e}")
        import traceback
        logger.error(f"❌ Stacktrace: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    main()