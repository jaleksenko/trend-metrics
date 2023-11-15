from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lower, regexp_replace, udf, explode
from pyspark.sql.types import StringType, StructField, StructType, TimestampType, ArrayType
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Download necessary NLTK components to the specified directory.
nltk.data.path.append('/opt/bitnami/spark/data/nltk_data')
nltk.download('punkt', download_dir='/opt/bitnami/spark/data/nltk_data')
nltk.download('stopwords', download_dir='/opt/bitnami/spark/data/nltk_data')


def remove_stopwords(text):
    # Set stop words in English
    stop_words = set(stopwords.words('english'))
    # Tokenize the text
    word_tokens = word_tokenize(text)
    # Check tokens
    print(f"Tokens: {word_tokens}")
    # Remove stop words
    filtered_words = [word for word in word_tokens if word not in stop_words]
    return filtered_words

# Register UDF in Spark
remove_stopwords_udf = udf(remove_stopwords, ArrayType(StringType()))

def main():
    # Create Spark session
    spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Read data from Kafka
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "feeds") \
        .load()

    # Convert Kafka data to strings
    messages_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # Define schema of Kafka message
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("published", TimestampType(), True),
        StructField("ChannelTitle", StringType(), True)
    ])

    # Parse and clean data
    parsed_df = messages_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
    cleaned_df = parsed_df.withColumn("title", lower(col("title"))) \
        .withColumn("title", regexp_replace(col("title"), "[^a-zA-Z0-9 ]", "")) \
        .withColumn("title", remove_stopwords_udf(col("title")))

    # Output cleaned data to the console
    query = cleaned_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()