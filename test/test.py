from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class SparkStreaming:
    def __init__(self, kafka_config, topics):
        self.spark = SparkSession.builder \
            .appName("KafkaSparkStreamingToHDFS") \
            .getOrCreate()
        
        self.kafka_config = kafka_config
        self.topics = topics
        
    def start_streaming(self):
        """Bắt đầu streaming từ Kafka và ghi JSON vào HDFS mỗi 5 phút"""
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_config['bootstrap.servers']) \
            .option("subscribe", ",".join(self.topics)) \
            .load()

        # Chuyển đổi giá trị từ bytes sang string
        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

        # Ghi vào HDFS mỗi 5 phút (300 giây)
        query = df.writeStream \
            .outputMode("append") \
            .format("json") \
            .option("path", "hdfs://master-node:9000/user/hadoop/kafka_json/") \
            .option("checkpointLocation", "hdfs://master-node:9000/user/hadoop/checkpoints/kafka_json/") \
            .trigger(processingTime="300 seconds") \
            .start()

        query.awaitTermination()


if __name__ == "__main__":
    KAFKA_CONFIG = {
        'bootstrap.servers': '192.168.235.143:9092',
        'client.id': 'python-client'
    }

    topic_name = "delivery-topic"

    streaming = SparkStreaming(KAFKA_CONFIG, [topic_name])
    streaming.start_streaming()
