from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Creating Spark session
    spark = SparkSession.builder.appName("KafkaSparkStreamingTest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Reading data from Kafka
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "feeds") \
        .load()

    # Converting Kafka data to strings
    messages_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # Outputting data to the console
    query = messages_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()





