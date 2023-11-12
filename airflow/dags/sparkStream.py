from pyspark.sql import SparkSession

# # Create a SparkSession
# spark = SparkSession.builder \
#     .appName("KafkaMessageConsumer") \
#     .getOrCreate()

# # Set the Kafka broker and topic you want to consume from
# kafka_broker = "localhost:9092"
# kafka_topic = "opensky_stream"

# # Read messages from Kafka using Structured Streaming
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_broker) \
#     .option("subscribe", kafka_topic) \
#     .load()

# # Define a query to print the messages to the console
# query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# # Await termination to keep the program running
# query.awaitTermination()

spark = SparkSession \
          .builder \
          .appName("APP") \
          .getOrCreate()

df = spark\
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "sparktest") \
      .option("startingOffsets", "earliest") \
      .load()
      

query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .option("checkpointLocation", "path/to/HDFS/dir") \
    .start()

query.awaitTermination()