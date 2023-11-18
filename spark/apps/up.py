from pyspark.sql import Row
import sparknlp

spark = sparknlp.start()
spark.conf.set("spark.cassandra.connection.host", "cassandra")
# spark.sparkContext.setLogLevel("DEBUG")

# Test data
test_data = [Row(date="2023-11-19", hour=11, keyword="test_keyword3", count=3),
             Row(date="2023-11-19", hour=11, keyword="test_keyword4", count=4)]

# Create a DataFrame from a list
test_df = spark.createDataFrame(test_data)

test_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(keyspace="keywords", table="keywords_hour") \
    .save()
