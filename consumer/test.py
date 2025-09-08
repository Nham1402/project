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
    StructField("created_timestamp", StringType(), True),   # debug để xem raw string
    StructField("processed_timestamp", StringType(), True),
    StructField("updated_timestamp", StringType(), True)
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

            # In raw message trước để debug
            raw_df = df.selectExpr("CAST(value AS STRING) as raw_message")

            query_raw = raw_df.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .start()

            logger.info("👀 Raw Kafka message printing...")

            # Nếu raw OK → parse JSON
            transactions_df = raw_df.select(
                from_json(col("raw_message"), TRANSACTION_SCHEMA).alias("data")
            ).select("data.*")

            query_json = transactions_df.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .start()

            logger.info("🚀 JSON parsing stream started.")

            query_raw.awaitTermination()
            query_json.awaitTermination()

        except Exception as e:
            logger.error(f"❌ Error in streaming: {e}")
            traceback.print_exc()
        finally:
            self.spark.stop()
            logger.info("🛑 Spark session stopped.")


if __name__ == "__main__":
    app = RealTimeStreaming()
    app.start_streaming()
