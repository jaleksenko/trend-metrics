import sparknlp
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.sql.functions import col


# Функция для преобразования данных с помощью TF-IDF
def tfidf_transform(df, inputCol="cleanTokens", numFeatures=20000):
    tokenizer = Tokenizer(inputCol=inputCol, outputCol="tokens")
    hashingTF = HashingTF(inputCol="tokens", outputCol="rawFeatures", numFeatures=numFeatures)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf])
    model = pipeline.fit(df)
    return model.transform(df)

# Инициализация Spark сессии
# spark = SparkSession.builder.appName("TF-IDF Test").getOrCreate()
spark = sparknlp.start()

# Создание тестовых данных
data = [("token1 token2 token3".split(" "),),
        ("token2 token4".split(" "),)]
columns = ["cleanTokens"]

df = spark.createDataFrame(data, columns)

# Применение TF-IDF преобразования
tfidf_df = tfidf_transform(df)

# Вывод результатов
tfidf_df.select("cleanTokens", "features").show(truncate=False)

# Завершение Spark сессии
spark.stop()
