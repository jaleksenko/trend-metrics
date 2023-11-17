
def read_from_kafka(spark, servers, topic):
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    return kafka_df.selectExpr("CAST(value AS STRING)")
