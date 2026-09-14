import os
import sys

from pyspark.sql import SparkSession

# Python executable for PySpark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("Spark Stages Assignment")
    .master("local[*]")
    .getOrCreate()
)

print("Spark session started!")

data = [
    ("Tanishq", "Delhi", 1000),
    ("Rahul", "Mumbai", 2000),
    ("Priya", "Delhi", 1500),
    ("Amit", "Mumbai", 3000)
]

df = spark.createDataFrame(
    data,
    ["name", "city", "amount"]
)

# a. Filter the records where amount > 1500.

df2 = df.filter(df.amount > 1500) # narrow transformation

# b. Select only name, city, and amount

df3 = df2.select(
    "name",
    "city",
    "amount"
) # narrow transformation

# c. Add tax = 18% of amount

df4 = df3.withColumn(
    "tax",
    df3.amount * 0.18
)# narrow transformation

# d. Group by city and calculate total sales

df5 = df4.groupBy("city").sum("amount") # wide transformation

# e. Display final result

df5.show()


input("Press Enter to stop Spark...")

spark.stop()
