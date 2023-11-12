# Spark Test Script
from pyspark.sql import SparkSession

def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Spark Test Script") \
        .getOrCreate()

    # Create a simple DataFrame
    data = [("John", 28), ("Linda", 33), ("Michael", 22)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)

    # Simple transformation: filter records where age is greater than 25
    df_filtered = df.filter(df.Age > 31)

    # Save the results to a file
    # coalesce(1) return 1 partition
    df_filtered.coalesce(1).write.csv("/opt/bitnami/spark/apps/spark_test_results.csv", header=True, mode="overwrite")

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    main()




from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def process_time(time, rdd):
    # Получаем количество сообщений в RDD
    count = rdd.count()
    if count > 0:
        with open("/opt/bitnami/spark/apps/kafka_result.txt", "a") as file:
            file.write(f"{time}: Received {count} messages\n")

if __name__ == "__main__":
    # Создание Spark Context
    sc = SparkContext(appName="KafkaSparkStreaming")
    sc.setLogLevel("WARN")

    # Создание Streaming Context с интервалом батча 15 минут
    ssc = StreamingContext(sc, 900)

    # Параметры Kafka
    kafkaParams = {"metadata.broker.list": "kafka:9092"}
    topic = "feeds"

    # Создание DStream для чтения данных из Kafka
    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams)

    # Обработка данных из Kafka и запись количества сообщений в файл
    kafkaStream.foreachRDD(process_time)

    # Запуск потоковой обработки
    ssc.start()
    # Ожидание завершения
    ssc.awaitTermination()