from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("checkdata") \
    .getOrCreate()
    
df = spark.read.json("hdfs://master-node:9000/user/hadoop/kafka_json/part-00000-59c73b80-6657-4d85-952c-b51047045349-c000.json")

df.printSchema()

df.show(truncate=False)