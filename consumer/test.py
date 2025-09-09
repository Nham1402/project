from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace
from pyspark.sql.types import *
import logging
import traceback

# ================== Logging ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ================== Kafka config ==================
KAFKA_CONFIG = {
    "bootstrap.servers": "192.168.235.136:9092",  
    "topic": "transaction_data"
}

# ================== Schema ==================
TRANSACTION_SCHEMA = StructType([
    StructField("account_key", IntegerType(), True),
    StructField("customer_key", IntegerType(), True),
    StructField("location_key", IntegerType(), True),
    StructField("event_key", StringType(), True),  
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

# ================== Streaming App ==================
class RealTimeStreaming():
    def __init__(self):
        # Removed .master("yarn") - let spark-submit handle this
        self.spark = SparkSession.builder \
            .appName("RealtimeKafkaConsole") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark Session created.")

    def start_streaming(self):
        try:
            # Read from Kafka
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap.servers"]) \
                .option("subscribe", KAFKA_CONFIG["topic"]) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .option("kafka.session.timeout.ms", "30000") \
                .option("kafka.request.timeout.ms", "40000") \
                .load()

            logger.info("📡 Kafka stream loaded.")

            # Convert value -> string -> fix quotes -> parse JSON
            transactions_df = df.selectExpr("CAST(value AS STRING) as json_str") \
                .withColumn("json_str", regexp_replace("json_str", "'", "\"")) \
                .select(from_json(col("json_str"), TRANSACTION_SCHEMA).alias("data")) \
                .select("data.*") \
                .filter(col("transaction_id").isNotNull())

            logger.info("🔄 Data transformed to structured format.")

            # Simple console output for YARN cluster
            query = transactions_df.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .option("numRows", 20) \
                .trigger(processingTime="10 seconds") \
                .start()

            logger.info("🚀 Streaming query started.")
            query.awaitTermination()

        except Exception as e:
            logger.error(f"❌ Error in streaming: {e}")
            traceback.print_exc()
        finally:
            self.spark.stop()
            logger.info("🛑 Spark session stopped.")

# ================== Main ==================
if __name__ == "__main__":
    try:
        logger.info("🚀 Starting Realtime Kafka Streaming App...")
        app = RealTimeStreaming()
        app.start_streaming()
    except Exception as e:
        logger.error(f"❌ Failed to start application: {e}")
        traceback.print_exc()