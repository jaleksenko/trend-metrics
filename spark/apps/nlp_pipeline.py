import logging

from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import Tokenizer, Normalizer, LemmatizerModel, StopWordsCleaner, NGramGenerator
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.sql.functions import from_json, explode, col, to_timestamp, collect_list, desc, date_format, current_timestamp

from cassandra_tables import create_cassandra_table


# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def write_to_cassandra(batch_df, batch_id, table="keywords_hour", keyspace="keywords"):
    if not batch_df.rdd.isEmpty():

        # Add a new hour column
        batch_df_hour = batch_df.withColumn("hour", date_format(current_timestamp(), "yyyy-MM-dd HH"))

        # Write to Cassandra
        batch_df_hour.write \
                     .format("org.apache.spark.sql.cassandra") \
                     .options(table=table, keyspace=keyspace, ttl="172800") \
                     .mode("append") \
                     .save()

        logging.info(f"Batch {batch_id} successfully written to Cassandra.")


# def write_to_cassandra(batch_df, batch_id, table="keywords_hour", keyspace="keywords"):
#     if not batch_df.rdd.isEmpty():
#         # Add timestamp column
#         batch_df_with_timestamp = batch_df.withColumn("date", current_timestamp())

#         # Add UUID column
#         batch_df_with_uuid = batch_df_with_timestamp.withColumn("id", expr("uuid()"))

#         # Remove duplicates if necessary (if you have a logic for it)
#         # unique_batch_df = batch_df_with_uuid.distinct()

#         # Direct write to Cassandra
#         batch_df_with_uuid.write \
#                           .format("org.apache.spark.sql.cassandra") \
#                           .options(table=table, keyspace=keyspace, ttl="172800") \
#                           .mode("append") \
#                           .save()

#         logging.info(f"Batch {batch_id} successfully written to Cassandra.")

def nlp_pipeline(input_col):
    document_assembler = DocumentAssembler().setInputCol(input_col).setOutputCol("document")
    tokenizer = Tokenizer().setInputCols(["document"]).setOutputCol("token")
    normalizer = Normalizer().setInputCols(["token"]).setOutputCol("normalized").setLowercase(True)
    stopwords_cleaner = StopWordsCleaner.pretrained('stopwords_en', 'en').setInputCols(["normalized"]).setOutputCol("cleanTokens").setCaseSensitive(False)
    lemmatizer = LemmatizerModel.pretrained().setInputCols(["cleanTokens"]).setOutputCol("lemma")
    ngram_generator = NGramGenerator().setInputCols(["lemma"]).setOutputCol("ngrams").setN(2)
    finisher_lemma = Finisher().setInputCols(["lemma"]).setOutputCols(["lemma_features"]).setOutputAsArray(True).setCleanAnnotations(False)
    finisher_ngrams = Finisher().setInputCols(["ngrams"]).setOutputCols(["ngram_features"]).setOutputAsArray(True).setCleanAnnotations(False)

    return Pipeline(stages=[
        document_assembler, tokenizer, normalizer, stopwords_cleaner, lemmatizer, ngram_generator, finisher_lemma, finisher_ngrams
    ])


def process_stream_data(df, pipeline_model):
    processed_df = pipeline_model.transform(df)

    df_unigrams = processed_df.select(explode(col("lemma_features")).alias("keyword")).groupBy("keyword").count()
    df_bigrams = processed_df.select(explode(col("ngram_features")).alias("bigram")).groupBy("bigram").count()
    
    return df_unigrams, df_bigrams


def main():
    try:
        # Create a Cassandra table
        create_cassandra_table()

        # Create a Spark session
        spark = SparkSession.builder \
            .appName("SparkApp") \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                    "com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4,"
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"
                ) \
            .config("spark.jars.ivy", "/root/.ivy2") \
            .config("spark.jars.maven", "/root/.m2") \
            .getOrCreate()
        
        # Set Spark log level
        spark.sparkContext.setLogLevel("WARN")

        # Set the checkpoint directory
        spark.sparkContext.setCheckpointDir("/app/spark/checkpoints")

        # Set Cassandra configuration
        spark.conf.set("spark.cassandra.connection.host", "cassandra")
        
        # Create an empty DataFrame with a 'titles' column
        empty_df = spark.createDataFrame([], StructType([StructField("titles", StringType())]))
        
        # Create and train an empty NLP pipeline
        nlp_pipeline_model = nlp_pipeline("titles").fit(empty_df)

        # Read data from Kafka
        kafka_df = spark.readStream.format("kafka") \
                    .option("kafka.bootstrap.servers", "broker:29092") \
                    .option("subscribe", "feeds") \
                    .option("startingOffsets", "latest") \
                    .option("enable.auto.commit", "false") \
                    .option("failOnDataLoss", "false") \
                    .load()
    
        # Processing each batch of data
        def foreach_batch_function(batch_df, batch_id, nlp_pipeline_model):
            # Check for an empty batch
            if batch_df.rdd.isEmpty():
                logging.info(f"Empty batch {batch_id}, skipping processing.")
                return
                
            # Convert binary data to string and JSON to DataFrame
            schema = StructType([
                StructField("id", StringType(), True),
                StructField("title", StringType(), True),
                StructField("published", TimestampType(), True),
                StructField("ChannelTitle", StringType(), True)
            ])
            batch_df = batch_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                            .select(from_json(col("value"), schema).alias("data")) \
                            .select("data.*") \
                            .withColumn("published", to_timestamp("published"))
    
            # Apply time windows and aggregation
            aggregated_df = batch_df.groupBy().agg(collect_list("title").alias("titles"))
        
            # Apply the NLP pipeline to the data
            processed_df = nlp_pipeline_model.transform(aggregated_df)

            # Process the results of the NLP pipeline
            df_unigrams, df_bigrams = process_stream_data(processed_df, nlp_pipeline_model)
            
            # Combine unigrams and bigrams
            df_combined = df_unigrams.union(df_bigrams).withColumnRenamed("bigram", "keyword")

            # Find top keywords
            df_top_keywords = df_combined.orderBy(desc("count")).limit(20)

            # Write to Cassandra
            write_to_cassandra(df_top_keywords, batch_id)


        # Define the Kafka stream query
        query = kafka_df.writeStream \
            .outputMode("append") \
            .foreachBatch(lambda df, id: foreach_batch_function(df, id, nlp_pipeline_model)) \
            .option("checkpointLocation", "/app/spark/stream_checkpoints") \
            .trigger(processingTime='1 hours') \
            .start()
        query.awaitTermination()

    except Exception as e:
        logging.error(f"Error occurred: {e}")    

if __name__ == "__main__":
    main()
