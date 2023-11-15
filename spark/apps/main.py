import sparknlp
from data_reading import read_from_kafka
from data_cleaning import clean_data
from nlp_processing import nlp_pipeline_setup

from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.sql.functions import col, from_json

def main():
    # Start Spark session with Spark NLP
    spark = sparknlp.start()
    spark.sparkContext.setLogLevel("WARN")

    # Reading data from Kafka
    kafka_df = read_from_kafka(spark, "broker:29092", "feeds")

    # Define the schema of the Kafka message
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("published", TimestampType(), True),
        StructField("ChannelTitle", StringType(), True)
    ])

    # Parsing data
    parsed_df = kafka_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
    cleaned_df = clean_data(parsed_df)
    
    # Set up the Spark NLP pipeline
    nlp_pipeline = nlp_pipeline_setup()

    # Apply the pipeline to the data
    processed_df = nlp_pipeline.fit(cleaned_df).transform(cleaned_df)

    # Output processed data to the console
    query = processed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()


