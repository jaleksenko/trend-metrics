from pyspark.sql.functions import col, lower, regexp_replace

def clean_data(df):
    return df.withColumn("title", lower(col("title"))) \
        .withColumn("title", regexp_replace(col("title"), "[^a-zA-Z0-9 ]", ""))
