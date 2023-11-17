from pyspark.sql.functions import col, udf
from pyspark.ml.feature import IDF, CountVectorizer
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, FloatType
from pyspark.ml import Pipeline
from pyspark.ml.linalg import SparseVector


def extract_token_text(tokens):
    if tokens:
        return [token.result for token in tokens if 'result' in token]
    else:
        return []


def extract_token_text_udf():
    return udf(extract_token_text, ArrayType(StringType()))




def tfidf_transform(df, inputCol="tokens_str", numFeatures=20000):
    # CountVectorizer создает векторы частоты слов
    cv = CountVectorizer(inputCol=inputCol, outputCol="rawFeatures", vocabSize=numFeatures)

    # IDF используется для масштабирования векторов частоты с использованием обратной частоты документов
    idf = IDF(inputCol="rawFeatures", outputCol="features")

    # Определяем этапы пайплайна: CountVectorizer, а затем IDF
    pipeline = Pipeline(stages=[cv, idf])

    # Обучаем модель на данных
    model = pipeline.fit(df)

    # Применяем обученную модель к DataFrame
    tfidf_df = model.transform(df)

    # Сохраняем модель CountVectorizer для дальнейшего использования
    cv_model = model.stages[0]

    return tfidf_df, cv_model

# Определение схемы для нового столбца DataFrame
schema = ArrayType(StructType([
    StructField("keyword", StringType(), False),
    StructField("weight", FloatType(), False)
]))

def extract_keywords(features, vocabulary, num_keywords):
    # Преобразование разреженного вектора в список
    if isinstance(features, SparseVector):
        features = features.toArray().tolist()

    # Получение индексов и значений с наибольшими весами
    top_indices_weights = sorted(enumerate(features), key=lambda x: x[1], reverse=True)[:num_keywords]
   
    # Получение ключевых слов и их весов
    keywords = [(vocabulary[index], weight) for index, weight in top_indices_weights]
    return keywords


def extract_keywords_udf(vocabulary, num_keywords):
    # Создание UDF для применения функции к DataFrame
    return udf(lambda features: extract_keywords(features, vocabulary, num_keywords), schema)

