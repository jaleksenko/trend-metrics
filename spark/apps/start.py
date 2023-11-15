from pyspark.sql.functions import col, from_json, lower, regexp_replace
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.ml import Pipeline

from sparknlp.base import DocumentAssembler
from sparknlp.annotator import Tokenizer, SentenceDetector, StopWordsCleaner
import sparknlp

def main():
    # Start Spark session with Spark NLP
    spark = sparknlp.start()
    print("Spark NLP version", sparknlp.version())
    print("Apache Spark version:", spark.version)
    spark.sparkContext.setLogLevel("WARN")

    # Reading data from Kafka
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "feeds") \
        .load()

    # Converting Kafka data to strings
    messages_df = kafka_df.selectExpr("CAST(value AS STRING)")

    # Define the schema of the Kafka message
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("published", TimestampType(), True),
        StructField("ChannelTitle", StringType(), True)
    ])

    # Parsing data
    parsed_df = messages_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
    cleaned_df = parsed_df.withColumn("title", lower(col("title"))) \
        .withColumn("title", regexp_replace(col("title"), "[^a-zA-Z0-9 ]", ""))
    
    # Set up the Spark NLP pipeline
    document = DocumentAssembler() \
        .setInputCol("title") \
        .setOutputCol("document")

    sentence = SentenceDetector() \
        .setInputCols(["document"]) \
        .setOutputCol("sentence")

    token = Tokenizer() \
        .setInputCols(["sentence"]) \
        .setOutputCol("token")

    stop_words = StopWordsCleaner.pretrained('stopwords_en', 'en') \
        .setInputCols(["token"]) \
        .setOutputCol("cleanTokens") \
        .setCaseSensitive(False)

    # Define the pipeline
    nlp_pipeline = Pipeline(stages=[
        document,
        sentence,
        token,
        stop_words
    ])

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
