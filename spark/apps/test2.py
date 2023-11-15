from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, from_json
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

if __name__ == "__main__":
    # Creating Spark session
    spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Reading data from Kafka
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "feeds") \
        .load()

    # Converting Kafka data to strings
    messages_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # Define the schema of the Kafka message
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("published", TimestampType(), True),
        StructField("ChannelTitle", StringType(), True)
    ])

    # Parsing and cleaning data
    parsed_df = messages_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
    cleaned_df = parsed_df.withColumn("title", lower(col("title"))) \
        .withColumn("title", regexp_replace(col("title"), "[^a-zA-Z0-9 ]", ""))

    # Outputting cleaned data to the console
    query = cleaned_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()