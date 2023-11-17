import sparknlp
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import Tokenizer, Normalizer, LemmatizerModel, StopWordsCleaner, NGramGenerator

from pyspark.ml import Pipeline
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.sql.functions import from_json, explode, col, length, to_timestamp, window, collect_list

import datetime
from pyspark.sql.functions import lit
from cassandra import create_cassandra_table

# Function to write batch data to Cassandra
def write_to_cassandra(batch_df, 
                       table="tokens_hour", 
                       keyspace="your_keyspace",
                       ttl="10800"):

    # Obtain the current date and hour for data grouping
    current_time = datetime.datetime.now()
    date_str = current_time.strftime("%Y-%m-%d")
    hour_int = current_time.hour

    # Add columns for date and hour to the DataFrame
    batch_df_with_time = batch_df.withColumn("date", lit(date_str)) \
                                .withColumn("hour", lit(hour_int))

    # Group by date, hour, and token, then count mentions
    aggregated_df = batch_df_with_time.groupBy("date", "hour", "token") \
                                      .count()

    # Write the aggregated data to Cassandra with a TTL (Time-To-Live)
    (aggregated_df.write
                  .format("org.apache.spark.sql.cassandra")
                  .options(table=table, keyspace=keyspace)
                  .option("ttl", ttl)  # TTL in seconds
                  .mode("append")
                  .save())

# Function to create an NLP pipeline with Spark NLP
def nlp_pipeline(input_col):
    # Initialize various NLP components
    document_assembler = DocumentAssembler().setInputCol(input_col).setOutputCol("document")
    tokenizer = Tokenizer().setInputCols(["document"]).setOutputCol("token")
    normalizer = Normalizer().setInputCols(["token"]).setOutputCol("normalized").setLowercase(True)
    stopwords_cleaner = StopWordsCleaner.pretrained('stopwords_en', 'en').setInputCols(["normalized"]).setOutputCol("cleanTokens").setCaseSensitive(False)
    lemmatizer = LemmatizerModel.pretrained().setInputCols(["cleanTokens"]).setOutputCol("lemma")
    ngram_generator = NGramGenerator().setInputCols(["lemma"]).setOutputCol("ngrams").setN(2)
    finisher_lemma = Finisher().setInputCols(["lemma"]).setOutputCols(["lemma_features"]).setOutputAsArray(True).setCleanAnnotations(False)
    finisher_ngrams = Finisher().setInputCols(["ngrams"]).setOutputCols(["ngram_features"]).setOutputAsArray(True).setCleanAnnotations(False)

    # Return the constructed NLP pipeline
    return Pipeline(stages=[
        document_assembler, tokenizer, normalizer, stopwords_cleaner, lemmatizer, ngram_generator, finisher_lemma, finisher_ngrams
    ])

def main():
    
    # Create Cassandra tables
    create_cassandra_table()

    # Initialize Spark session with Spark NLP
    spark = sparknlp.start()
    spark.sparkContext.setLogLevel("WARN")

    # Read streaming data from Kafka
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "feeds") \
        .option("startingOffsets", "latest") \
        .load()

    # Convert Kafka data to string format
    messages_df = kafka_df.selectExpr("CAST(value AS STRING)")

    # Define the schema for Kafka message data
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("published", TimestampType(), True),
        StructField("ChannelTitle", StringType(), True)
    ])

    # Parse the data using the defined schema
    df = messages_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
    
    # Convert 'published' column to timestamp format
    df = df.withColumn("published", to_timestamp("published"))

    # Data processing with watermark and window functions
    watermarked_df = df.withWatermark("published", "1 hour 10 minutes")
    windowed_df_24h = watermarked_df.groupBy(window(col("published"), "1 hour")).agg(collect_list("title").alias("titles"), collect_list("id").alias("ids"))

    # Set up the NLP pipeline and process the data
    pipeline = nlp_pipeline("titles")
    model = pipeline.fit(windowed_df_24h)
    processed_df = model.transform(windowed_df_24h)

    # Expand DataFrame to include 'id' column
    processed_df = processed_df.withColumn("id", explode(col("ids")))

    # Convert lists of unigrams and bigrams to strings
    df_unigrams = processed_df.select("id", explode(col("lemma_features")).alias("token")).filter(length(col("token")) > 1)
    df_bigrams = processed_df.select("id", explode(col("ngram_features")).alias("token")).filter(length(col("token")) > 1)

    # Combine unigrams and bigrams into a single DataFrame
    df_combined = df_unigrams.union(df_bigrams)

    # Save the tokenization results to Cassandra
    query = df_combined.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_cassandra) \
        .trigger(processingTime='10 minutes') \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
