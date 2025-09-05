from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv
import os
import logging

# ===============================
# Logging setup - Thêm chi tiết hơn
# ===============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load biến môi trường từ .env
load_dotenv(dotenv_path=os.path.join("/home/hadoop/project", ".env"))

# ===============================
# Config - Thêm validation
# ===============================
KAFKA_CONFIG = {
    'bootstrap.servers': '192.168.235.136:9092,192.168.235.147:9092,192.168.235.148:9092',
}

# Kiểm tra các biến môi trường
required_env_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASS']
for var in required_env_vars:
    if not os.getenv(var):
        raise ValueError(f"❌ Thiếu biến môi trường: {var}")

POSTGRES_CONFIG = {
    "url": f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
    "table": "banking_dw.fact_transaction",
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "driver": "org.postgresql.Driver"
}

# Log config để debug
logger.info(f"📊 Database URL: {POSTGRES_CONFIG['url']}")
logger.info(f"📊 Database Table: {POSTGRES_CONFIG['table']}")

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
# Test kết nối database
# ===============================
def test_database_connection(spark):
    """Test kết nối database trước khi streaming"""
    try:
        logger.info("🔍 Kiểm tra kết nối database...")
        
        # Tạo DataFrame test đơn giản
        test_data = [(1, "test", 100.0)]
        test_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("amount", DoubleType(), True)
        ])
        
        test_df = spark.createDataFrame(test_data, test_schema)
        
        # Thử đọc từ database (không cần table tồn tại)
        try:
            spark.read \
                .format("jdbc") \
                .option("url", POSTGRES_CONFIG["url"]) \
                .option("user", POSTGRES_CONFIG["user"]) \
                .option("password", POSTGRES_CONFIG["password"]) \
                .option("driver", POSTGRES_CONFIG["driver"]) \
                .option("query", "SELECT 1 as test") \
                .load() \
                .show()
            
            logger.info("✅ Kết nối database thành công!")
            return True
            
        except Exception as db_error:
            logger.error(f"❌ Lỗi kết nối database: {db_error}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Lỗi test database: {e}")
        return False

# ===============================
# Spark Streaming class - Cải thiện
# ===============================
class SparkStreaming:
    def __init__(self, kafka_config, topics):
        self.spark = SparkSession.builder \
            .appName("KafkaSparkToPostgres") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        self.kafka_config = kafka_config
        self.topics = topics
        
        # Test kết nối database
        if not test_database_connection(self.spark):
            raise Exception("Không thể kết nối database")

    def read_stream(self):
        try:
            logger.info(f"📡 Đang kết nối Kafka: {self.kafka_config['bootstrap.servers']}")
            logger.info(f"📡 Topics: {self.topics}")
            
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_config['bootstrap.servers']) \
                .option("subscribe", ",".join(self.topics)) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .option("maxOffsetsPerTrigger", "1000") \
                .load()

            # Thêm debug để xem raw data từ Kafka
            def debug_kafka_data(batch_df, batch_id):
                logger.info(f"🔍 Debug Kafka Batch {batch_id}")
                logger.info(f"📊 Raw Kafka records: {batch_df.count()}")
                if batch_df.count() > 0:
                    batch_df.select("key", "value", "timestamp").show(5, truncate=False)

            # Debug stream (tạm thời)
            debug_query = df.writeStream \
                .foreachBatch(debug_kafka_data) \
                .outputMode("append") \
                .option("checkpointLocation", "/tmp/debug_checkpoint") \
                .trigger(processingTime="30 seconds") \
                .start()

            # Xử lý dữ liệu chính
            processed_df = df.select(
                col("key").cast("string"),
                from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ).select("key", "data.*", "kafka_timestamp")

            logger.info("✅ Kafka stream đã được tạo thành công")
            return processed_df

        except Exception as e:
            logger.error(f"❌ Lỗi khi tạo Kafka stream: {e}")
            raise

# ===============================
# Write batch to Postgres - Cải thiện
# ===============================
def write_to_postgres(batch_df, batch_id):
    try:
        logger.info(f"🔄 Bắt đầu xử lý Batch {batch_id}")
        
        # Debug: In schema và sample data
        logger.info(f"📊 Schema: {batch_df.schema}")
        
        row_count = batch_df.count()
        logger.info(f"📊 Batch {batch_id}: {row_count} records")
        
        if row_count > 0:
            # Hiển thị sample data
            logger.info("📋 Sample data:")
            batch_df.show(5, truncate=False)
            
            # Kiểm tra data quality
            valid_transactions = batch_df.filter(
                col("transaction_id").isNotNull() &
                col("transaction_amount").isNotNull() &
                (col("transaction_amount") > 0)
            )
            
            valid_count = valid_transactions.count()
            logger.info(f"📊 Valid records: {valid_count}/{row_count}")
            
            if valid_count > 0:
                # Thêm timestamp
                final_df = valid_transactions.withColumn("created_at", current_timestamp())
                
                logger.info(f"💾 Đang ghi {valid_count} records vào database...")
                
                final_df.write \
                    .format("jdbc") \
                    .option("url", POSTGRES_CONFIG["url"]) \
                    .option("dbtable", POSTGRES_CONFIG["table"]) \
                    .option("user", POSTGRES_CONFIG["user"]) \
                    .option("password", POSTGRES_CONFIG["password"]) \
                    .option("driver", POSTGRES_CONFIG["driver"]) \
                    .option("batchsize", "1000") \
                    .option("isolationLevel", "NONE") \
                    .option("numPartitions", "1") \
                    .mode("append") \
                    .save()

                logger.info(f"✅ Đã ghi thành công batch {batch_id} - {valid_count} records")
            else:
                logger.warning(f"⚠️ Batch {batch_id}: Không có dữ liệu hợp lệ")
        else:
            logger.info(f"⚠️ Batch {batch_id} trống, bỏ qua")

    except Exception as e:
        logger.error(f"❌ Lỗi khi ghi batch {batch_id}: {e}")
        # Log chi tiết lỗi
        import traceback
        logger.error(f"❌ Stacktrace: {traceback.format_exc()}")

# ===============================
# Main - Cải thiện
# ===============================
def main():
    try:
        logger.info("🚀 Khởi tạo Spark Streaming...")
        
        # Tạo thư mục checkpoint nếu chưa tồn tại
        checkpoint_dir = "/user/hadoop/checkpoints/transaction_data"
        logger.info(f"📁 Checkpoint directory: {checkpoint_dir}")
        
        stream = SparkStreaming(KAFKA_CONFIG, ['transaction_data'])
        df = stream.read_stream()

        # Data quality checks
        df_filtered = df.filter(
            col("transaction_id").isNotNull() &
            col("transaction_amount").isNotNull() &
            (col("transaction_amount") > 0)
        )

        query = df_filtered.writeStream \
            .foreachBatch(write_to_postgres) \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_dir) \
            .trigger(processingTime="10 seconds") \
            .start()

        logger.info("✅ Streaming đã bắt đầu. Nhấn Ctrl+C để dừng...")
        
        # Monitor streaming
        while query.isActive:
            progress = query.lastProgress
            if progress:
                logger.info(f"📊 Streaming Progress: {progress}")
            query.awaitTermination(timeout=30)

    except KeyboardInterrupt:
        logger.info("🛑 Đang dừng streaming...")
        if 'query' in locals():
            query.stop()
    except Exception as e:
        logger.error(f"❌ Lỗi trong main: {e}")
        import traceback
        logger.error(f"❌ Stacktrace: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    main()