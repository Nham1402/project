from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv
import os
import logging

# ===============================
# Logging setup
# ===============================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load biến môi trường từ .env
load_dotenv(dotenv_path=os.path.basename("/home/hadoop/project/.env"))

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
# Spark Streaming class
# ===============================
class SparkStreaming:
    def __init__(self, kafka_config, topics):
        self.spark = SparkSession.builder \
            .appName("KafkaSparkToPostgres") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        self.kafka_config = kafka_config
        self.topics = topics

    def read_stream(self):
        try:
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_config['bootstrap.servers']) \
                .option("subscribe", ",".join(self.topics)) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()

            df = df.select(
                col("key").cast("string"),
                from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ).select("key", "data.*", "kafka_timestamp")

            logger.info("✅ Kafka stream đã được tạo thành công")
            return df

        except Exception as e:
            logger.error(f"❌ Lỗi khi tạo Kafka stream: {e}")
            raise

# ===============================
# Write batch to Postgres
# ===============================
def write_to_postgres(batch_df, batch_id):
    try:
        row_count = batch_df.count()
        if row_count > 0:
            logger.info(f"🔄 Batch {batch_id}: {row_count} records")

            batch_df_with_timestamp = batch_df.withColumn("created_at", current_timestamp())

            batch_df_with_timestamp.write \
                .format("jdbc") \
                .option("url", POSTGRES_CONFIG["url"]) \
                .option("dbtable", POSTGRES_CONFIG["table"]) \
                .option("user", POSTGRES_CONFIG["user"]) \
                .option("password", POSTGRES_CONFIG["password"]) \
                .option("driver", POSTGRES_CONFIG["driver"]) \
                .option("batchsize", "1000") \
                .option("isolationLevel", "NONE") \
                .mode("append") \
                .save()

            logger.info(f"✅ Đã ghi thành công batch {batch_id}")
        else:
            logger.info(f"⚠️ Batch {batch_id} trống, bỏ qua")

    except Exception as e:
        logger.error(f"❌ Lỗi khi ghi batch {batch_id}: {e}")

# ===============================
# Main
# ===============================
def main():
    try:
        logger.info("🚀 Khởi tạo Spark Streaming...")
        stream = SparkStreaming(KAFKA_CONFIG, ['transaction_data'])
        df = stream.read_stream()

        # Data quality checks (sử dụng đúng cột schema)
        df_filtered = df.filter(
            col("transaction_id").isNotNull() &
            col("transaction_amount").isNotNull() &
            (col("transaction_amount") > 0)
        )

        query = df_filtered.writeStream \
            .foreachBatch(write_to_postgres) \
            .outputMode("append") \
            .option("checkpointLocation", "/user/hadoop/checkpoints/transaction_data") \
            .trigger(processingTime="10 seconds") \
            .start()

        logger.info("✅ Streaming đã bắt đầu. Nhấn Ctrl+C để dừng...")
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("🛑 Đang dừng streaming...")
    except Exception as e:
        logger.error(f"❌ Lỗi trong main: {e}")
        raise

if __name__ == "__main__":
    main()
