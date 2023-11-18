from pyspark.sql import SparkSession
import sparknlp

spark = sparknlp.start()
spark.conf.set("spark.cassandra.connection.host", "cassandra")
# spark.sparkContext.setLogLevel("WARN")

df_test = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(keyspace="keywords", table="keywords_hour") \
    .load()

df_test.show()
