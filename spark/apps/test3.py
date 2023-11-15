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


