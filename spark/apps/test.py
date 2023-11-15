from pyspark.sql import SparkSession
from pyspark.ml import Pipeline

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from sparknlp.annotator import *
from sparknlp.common import *
from sparknlp.base import *
import sparknlp
 
def main():
    # Initialize Spark session with Spark NLP
    # spark = SparkSession.builder \
    #     .appName("KafkaSparkStreaming") \
    #     .master("local[*]") \
    #     .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:3.4.3") \
    #     .config("spark.driver.memory", "16G") \
    #     .config("spark.kryoserializer.buffer.max", "2000M") \
    #     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    #     .getOrCreate()
    # Initialize Spark session with Spark NLP
    spark = sparknlp.start()
    print("Spark NLP version", sparknlp.version())
    print("Apache Spark version:", spark.version)

    # Set log level to DEBUG for detailed information
    spark.sparkContext.setLogLevel("WARN")

    document = DocumentAssembler()\
        .setInputCol("text")\
        .setOutputCol("document")

    sentence = SentenceDetector()\
        .setInputCols(['document'])\
        .setOutputCol('sentence')

    token = Tokenizer()\
        .setInputCols(['sentence'])\
        .setOutputCol('token')

    stop_words = StopWordsCleaner.pretrained('stopwords_en', 'en')\
        .setInputCols(["token"]) \
        .setOutputCol("cleanTokens") \
        .setCaseSensitive(False)

    prediction_pipeline = Pipeline(
        stages = [
            document,
            sentence,
            token,
            stop_words
        ]
    )
    # Test example
    # prediction_data = spark.createDataFrame([["Maria is a nice place, that's you done the world @ 12345."]]).toDF("text")
    # result = prediction_pipeline.fit(prediction_data).transform(prediction_data)
    # print("Result:", result.select("cleanTokens.result").show(1, False))
    # print("Stop Words:", stop_words.getStopWords())


if __name__ == "__main__":
    main()