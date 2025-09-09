from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv
import os


class SparkStreaming:
    
    def __init__(self):
        # Load biến môi trường từ file .env (nếu có)
        load_dotenv()

        # Lấy Kafka broker từ env, nếu không có thì dùng mặc định
        self.kafka_broker = os.getenv("KAFKA_BROKER", "192.168.235.136:9092")
        self.kafka_topic = os.getenv("KAFKA_TOPIC", "transaction_data")

        self.spark = SparkSession.builder \
            .appName("sparkStreamingKafka") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
    
    def start_streaming(self):
        """Bắt đầu streaming từ Kafka và ghi JSON vào console (demo)"""
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", self.kafka_topic) \
            .load()

        # Chuyển đổi key, value từ bytes sang string
        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

        # Ghi ra console (mỗi micro-batch)
        query = df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .start()

        query.awaitTermination()
        

if __name__ == "__main__":
    streaming = SparkStreaming()
    streaming.start_streaming()
