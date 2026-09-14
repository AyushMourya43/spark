import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Big Data Fundamentals") \
    .getOrCreate()

df = spark.createDataFrame([
    (1, "Laptop", 70000),
    (2, "Phone", 45000)
], ["id", "product", "price"])

# S1
df.filter(df.price > 50000).show()

# S2
df.groupBy("product").count().show()

df.show()

input("Press Enter to stop Spark...")

spark.stop()