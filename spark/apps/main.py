from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, from_json
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import Tokenizer, StopWordsCleaner
import sparknlp




def main():
    # Initialize Spark session with Spark NLP
    spark = sparknlp.start()
    print("Spark NLP version", sparknlp.version())
    print("Apache Spark version:", spark.version)

    # Reading data from Kafka
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "feeds") \
        .load()

    # Convert Kafka data to strings
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

    # Set up the Spark NLP pipeline
    document_assembler = DocumentAssembler() \
        .setInputCol("title") \
        .setOutputCol("document")

    tokenizer = Tokenizer() \
        .setInputCols(["document"]) \
        .setOutputCol("token")

    stop_words_cleaner = StopWordsCleaner.pretrained('stopwords_en', 'en') \
        .setInputCols(["token"]) \
        .setOutputCol("clean_tokens") \
        .setCaseSensitive(False)

    finisher = Finisher() \
        .setInputCols(["clean_tokens"]) \
        .setOutputCols(["final_tokens"]) \
        .setOutputAsArray(True)

    # Build the pipeline
    nlp_pipeline = Pipeline(stages=[
        document_assembler, 
        tokenizer, 
        stop_words_cleaner,
        finisher
    ])

    # Apply the pipeline to the data
    processed_df = nlp_pipeline.fit(parsed_df).transform(parsed_df)

    # Output processed data to the console
    query = processed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()

