from pyspark.sql import SparkSession
from pyspark.sql.functions import *

KAFKA_CONFIG = {
    'bootstrap.servers': '192.168.235.136:9092,192.168.235.147:9092,192.168.235.148:9092',
}

class SparkStreaming:
    def __init__(self, kafka_config, topics):
        self.spark = SparkSession.builder \
            .appName("KafkaSparkStreaming") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()

        self.kafka_config = kafka_config
        self.topics = topics

    def read_stream(self):
        """Đọc dữ liệu từ Kafka và tạo DataFrame"""
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_config['bootstrap.servers']) \
            .option("subscribe", ",".join(self.topics)) \
            .option("startingOffsets", "earliest") \
            .load()

        # Chuyển đổi từ binary sang string
        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
        return df


if __name__ == "__main__":
    stream = SparkStreaming(KAFKA_CONFIG, ['transaction_data'])
    df = stream.read_stream()

    # In dữ liệu ra console
    query = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()
