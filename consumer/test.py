from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import traceback

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_CONFIG = {
    "bootstrap.servers": "192.168.235.136:9092"  
}

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

class RealTimeStreaming():
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("RealtimeKafkaConsole") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        logger.info("✅ Spark Session created (local mode).")

    def start_streaming(self):
        try:
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap.servers"]) \
                .option("subscribe", "transaction_data") \
                .option("startingOffsets", "earliest") \
                .load()

            logger.info("📡 Kafka stream loaded.")

            transactions_df = df.selectExpr("CAST(value AS STRING) as json_str") \
                .select(from_json(col("json_str"), TRANSACTION_SCHEMA).alias("data")) \
                .select("data.*")

            logger.info("🔄 Data transformed to structured format.")

            query = transactions_df.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .start()

            logger.info("🚀 Streaming query started.")
            query.awaitTermination()

        except Exception as e:
            logger.error(f"❌ Error in streaming: {e}")
            traceback.print_exc()
        finally:
            self.spark.stop()
            logger.info("🛑 Spark session stopped.")

if __name__ == "__main__":
    app = RealTimeStreaming()
    app.start_streaming()
