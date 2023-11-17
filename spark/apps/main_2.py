import sparknlp
from data_reading import read_from_kafka
from data_cleaning import clean_data
from nlp_processing import nlp_pipeline_setup
from tfidf_processing import tfidf_transform, extract_keywords_udf

from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.sql.functions import col, from_json, to_timestamp, window, collect_list, concat_ws
from pyspark.ml.feature import CountVectorizer



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
    # Создание тестового DataFrame
    # test_data = [
    #     {"id": "3174ecee-f121-4150-8b39-49cbe2e21c94", "title": "CNBC Daily Open: The Moody’s was a non-event", "published": "2023-11-16T23:43:15Z", "ChannelTitle": "CNBC Energy"},
    #     {"id": "7f3f05f8-6efe-4d63-b9d1-5b7a0a5cae58", "title": "Tristan Thompson Says His Cheating Comes from Trauma", "published": "2023-11-16T05:31:00Z", "ChannelTitle": "The Messenger"},
    #     {"id": "4230bff0-9ec1-405b-b7bb-7e302917bd08", "title": "‘Sovereign Citizen’ Insists Stack of Papers Citizen Count as Real Driver’s License in Citizen Arrest", "published": "2023-11-16T05:36:12Z", "ChannelTitle": "The Messenger"}
    # ]

    # Чтение test_messages из файла
    import json
    file_path = 'test_messages.json'
    with open(file_path, 'r', encoding='utf-8') as file:
        test_data = json.load(file)
    # print(test_data)



    test_df = spark.createDataFrame(test_data)
    test_df = test_df.withColumn("published", to_timestamp("published"))
    cleaned_df = clean_data(test_df)
    # cleaned_df.show(truncate=False)

    # Обработка данных, аналогично вашему текущему пайплайну
    watermarked_df = cleaned_df.withWatermark("published", "1 hour")
    windowed_df_24h = watermarked_df.groupBy(window(col("published"), "1 day")).agg(collect_list("title").alias("titles"))
    windowed_df_24h = windowed_df_24h.withColumn("all_titles", concat_ws(" ", "titles"))

    # Применение NLP пайплайна
    nlp_pipeline = nlp_pipeline_setup()
    processed_windowed_df_24h = nlp_pipeline.fit(windowed_df_24h).transform(windowed_df_24h).select("window", "cleanTokens")
    
    # Вывод результатов
    # processed_windowed_df_24h.writeStream.outputMode("append").format("console").start().awaitTermination()
    # processed_windowed_df_24h.printSchema()
    # processed_windowed_df_24h.show(truncate=False)
    # processed_windowed_df_24h.select("all_titles").show(truncate=False)
    # processed_windowed_df_24h.select("window").show(truncate=False)
    # processed_windowed_df_24h.select("token").show(truncate=False)
    # processed_windowed_df_24h.select("cleanTokens").show(truncate=False)
    
    # Определяем CountVectorizer
    cv = CountVectorizer(inputCol="cleanTokens", outputCol="cvFeatures")
    
    # Обучаем модель на данных
    cv_model = cv.fit(processed_windowed_df_24h)

    # Преобразуем данные с помощью модели CountVectorizer
    cv_df = cv_model.transform(processed_windowed_df_24h)

    # Теперь можно применить TF-IDF преобразование  
    tfidf_df, cv_model = tfidf_transform(cv_df, inputCol="cvFeatures")

    # Определяем количество ключевых слов
    num_keywords = 5

    # Создаем UDF для извлечения ключевых слов
    keywords_udf = extract_keywords_udf(cv_model, num_keywords)

    # Применяем UDF
    keywords_df = tfidf_df.withColumn("keywords", keywords_udf(col("features")))


    keywords_df.printSchema()
    keywords_df.show(truncate=False)


    # Parsing data
    # parsed_df = kafka_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
    # cleaned_df = clean_data(parsed_df)
    
    # Transform column 'published' to timestamp and replace it
    # dateFormat = "yyyy-MM-dd HH:mm:ss"
    # cleaned_df = cleaned_df.withColumn("published", to_timestamp("published", dateFormat))
    # cleaned_df = cleaned_df.withColumn("published", to_timestamp("published"))
    # cleaned_df.printSchema()

    # Grouping data by 24 hours time window
    # windowedCounts_24h = cleaned_df.groupBy(
    #     window(col("published"), "1 day")
    # ).count()

    # Setup watermark
    # watermarked_df = cleaned_df.withWatermark("published", "1 hour")
    # watermarked_df = cleaned_df.withWatermark("published", "5 minutes")
    # windowed_df_24h = watermarked_df.groupBy(window(col("published"), "1 day")).agg(collect_list("title").alias("titles"))
    # windowed_df_24h.writeStream.outputMode("append").format("console").start().awaitTermination()
    # Grouping data by 24 hours time window and processing with NLP pipeline
 
    

    # windowed_df_24h = watermarked_df.groupBy(window(col("published"), "1 day"))
    # windowed_df_24h = windowed_df_24h.agg(collect_list("title").alias("titles"))
    # windowed_df_24h = windowed_df_24h.alias("titles"))
    # Combine headers into a single line
    # windowed_df_24h = windowed_df_24h.withColumn("all_titles", concat_ws(" ", "titles"))

    # Set up the Spark NLP pipeline
    # nlp_pipeline = nlp_pipeline_setup()

    # Apply the pipeline to the data
    # processed_df = nlp_pipeline.fit(windowed_df_24h).transform(windowed_df_24h)
    
    # processed_windowed_df_24h = nlp_pipeline.fit(windowed_df_24h).transform(windowed_df_24h.select("window", "all_titles"))
    
    # Apply TF-IDF transformation
    # tfidf_df = tfidf_transform(processed_df, inputCol="cleanTokens")

    # Apply TF-IDF transformation on each windowed data
    # tfidf_df_24h = tfidf_transform(processed_windowed_df_24h, inputCol="titles")


    
    # Output processed data to the console
    # query = processed_df.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .start()

    # query = tfidf_df_24h.writeStream.outputMode("complete").format("console").start()
    # query = cleaned_df.writeStream.outputMode("append").format("console").start()
    # query = windowed_df_24h.writeStream.outputMode("append").format("console").start()
    # query = processed_windowed_df_24h.writeStream.outputMode("append").format("console").start()
    

    
    # query = keywords_df.writeStream \
    # .outputMode("append") \
    # .format("console") \
    # .start()

    # query.awaitTermination()

if __name__ == "__main__":
    main()


