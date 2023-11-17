from sparknlp.base import DocumentAssembler
from sparknlp.annotator import Tokenizer, SentenceDetector, StopWordsCleaner
from pyspark.ml import Pipeline

def nlp_pipeline_setup():
    # DocumentAssembler takes 'all_titles' as the input column
    document = DocumentAssembler() \
        .setInputCol("all_titles") \
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
