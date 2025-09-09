from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from dotenv import load_dotenv
import os





class SparkStreaming:
    
    def __init__(self):
        self.spark = SparkSession.builder\
            .appName("sparkStreamingKafka")\
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
    
    def start_streaming(self):
        """Bắt đầu streaming từ Kafka và ghi JSON vào HDFS mỗi 5 phút"""
        df = self.spark.readStream \
            .format("kafka") \
            .option( "bootstrap.servers": "192.168.235.126:9092") \
            .option("subscribe", "transaction_data") \
            .load()

        # Chuyển đổi giá trị từ bytes sang string
        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

        # Ghi vào HDFS mỗi 5 phút (300 giây)
        query = df.writeStream \
            .outputMode("complete") \
            .format("console") \
            .start()

        query.awaitTermination()
        
if __name__ == "__main__":
    
    streaming = SparkStreaming()
    streaming.start_streaming()