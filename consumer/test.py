from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv
import os
import logging
import traceback

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables (.env đặt ngoài project/consumer)
load_dotenv(dotenv_path=os.path.join("/home/hadoop/project", ".env"))

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
            .appName("RealtimeKafkaToPostgres") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        logger.info("✅ Spark Session created.")

    def write_realtime_batch(self, batch_df, batch_id):
        try:
            logger.info(f"⚡ Batch {batch_id} received with {batch_df.count()} records")

            # In dữ liệu ra console
            batch_df.show(truncate=False)

            # Ghi dữ liệu vào Postgres
            batch_df.write \
                .format("jdbc") \
                .option("url", POSTGRES_CONFIG["url"]) \
                .option("dbtable", POSTGRES_CONFIG["table"]) \
                .option("user", POSTGRES_CONFIG["user"]) \
                .option("password", POSTGRES_CONFIG["password"]) \
                .option("driver", POSTGRES_CONFIG["driver"]) \
                .mode("append") \
                .save()

            logger.info(f"📥 Batch {batch_id} saved to Postgres.")

        except Exception as e:
            logger.error(f"❌ Error in write_realtime_batch: {e}")
            traceback.print_exc()

    def start_streaming(self):
        try:
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap.servers']) \
                .option("subscribe", "transaction_data") \
                .option("startingOffsets", "earliest") \
                .load()

            logger.info("📡 Kafka stream loaded.")

            # Parse JSON từ Kafka message
            transactions_df = df.selectExpr("CAST(value AS STRING) as json_str") \
                .select(from_json(col("json_str"), TRANSACTION_SCHEMA).alias("data")) \
                .select("data.*")

            logger.info("🔄 Data transformed to structured format.")

            # Streaming query
            query = transactions_df.writeStream \
                .foreachBatch(self.write_realtime_batch) \
                .option("checkpointLocation", "/home/hadoop/project/consumer/checkpoints/transaction_checkpoint") \
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
