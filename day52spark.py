import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("Partition Assignment")
    .master("local[*]")
    .getOrCreate()
)

print("Spark session started!")

data = [
    (1, "Delhi", 500),
    (2, "Mumbai", 700),
    (3, "Bangalore", 1200),
    (4, "Delhi", 800),
    (5, "Mumbai", 1500),
    (6, "Pune", 900),
    (7, "Delhi", 1100),
    (8, "Bangalore", 2000),
    (9, "Mumbai", 600),
    (10, "Pune", 1300),
    (11, "Delhi", 750),
    (12, "Mumbai", 1800),
    (13, "Bangalore", 950),
    (14, "Pune", 1600),
    (15, "Delhi", 2200),
    (16, "Mumbai", 1000),
    (17, "Bangalore", 1400),
    (18, "Pune", 700),
    (19, "Delhi", 1250),
    (20, "Mumbai", 2100)
]

df = spark.createDataFrame(
    data,
    ["customer_id", "city", "amount"]
)

df.show()

# Print the number of partitions.

print("Number of partitions:", df.rdd.getNumPartitions())

# Repartition the DataFrame into 4 partitions.

df2 = df.repartition(4)

# Print the number of partitions again.

print(
    "Number of partitions after repartitioning:",
    df2.rdd.getNumPartitions()
)

# Use spark_partition_id() to display which partition each record belongs to.

df2 = df2.withColumn(
    "partition_id",
    spark_partition_id()
)

df2.show(20)

input("Press Enter to stop Spark...")

