from sparknlp.base import DocumentAssembler
from sparknlp.annotator import Tokenizer, SentenceDetector, StopWordsCleaner
from pyspark.ml import Pipeline

def nlp_pipeline_setup():
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

    return Pipeline(stages=[document, sentence, token, stop_words])
