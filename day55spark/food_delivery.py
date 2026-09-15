import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col ,trim, upper
from pyspark.sql.types import IntegerType, DoubleType

# Part A — Data Extraction

spark = (
    SparkSession.builder
    .appName("FoodDeliveryDataCleaning")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("input/food_delivery_orders.csv")
)

print("First few records:")
df.show(5, truncate=False)

print("DataFrame Schema:")
df.printSchema()

print("Total records:", df.count())

# Part B — Data Inspection

for column in df.columns:
    missing_count = df.filter(col(column).isNull()).count()
    print(column, ":", missing_count)

# Task 7 — Specific missing records
print("Records with missing age:")
df.filter(col("age").isNull()).show(truncate=False)

print("Records with missing quantity:")
df.filter(col("quantity").isNull()).show(truncate=False)

print("Records with missing delivery fee:")
df.filter(col("delivery_fee").isNull()).show(truncate=False)

print("Records with missing customer rating:")
df.filter(col("customer_rating").isNull()).show(truncate=False)

# Task 8 — Invalid records
print("Negative age:")
df.filter(col("age") < 0).show(truncate=False)

print("Negative quantity:")
df.filter(col("quantity") < 0).show(truncate=False)

print("Rating greater than 5:")
df.filter(col("customer_rating") > 5).show(truncate=False)

# Part C — Column Selection
selected_df = df.select(
    "order_id",
    "customer_id",
    "customer_name",
    "age",
    "restaurant",
    "city",
    "food_price",
    "quantity",
    "delivery_fee",
    "order_status",
    "customer_rating",
    "is_active"
)

selected_df.show(truncate=False)

# Part D — Data Type Cleaning
df = df.withColumn("age", col("age").cast(IntegerType()))

df = df.withColumn("food_price", col("food_price").try_cast(DoubleType()))

df = df.withColumn("quantity", col("quantity").cast(IntegerType()))

df = df.withColumn("delivery_fee", col("delivery_fee").cast(DoubleType()))

df = df.withColumn("customer_rating", col("customer_rating").cast(DoubleType()))

df.printSchema()


# Part E — Handling Invalid Data

df = df.filter(col("age").isNull() | (col("age") >= 18))
df = df.filter(col("food_price") > 0)
df = df.filter(col("quantity").isNull() | (col("quantity") > 0))
df = df.filter(col("customer_rating").isNull() | (col("customer_rating") <= 5))

df.show(truncate=False)

# Part F — Handling Missing Values

df = df.na.fill({"quantity": 1, "delivery_fee": 0})

df.show(truncate=False)

# Part G — Data Standardization
df = df.withColumn("city", trim(upper(col("city"))))

df.show(truncate=False)

# Part H — Column Renaming
df = df.withColumnRenamed("food_price", "item_price")

df.show(truncate=False)

# Part I — Derived Columns
df = df.withColumn("food_amount", col("item_price") * col("quantity"))
df = df.withColumn("total_amount", col("food_amount") + col("delivery_fee"))

df.show(truncate=False)

# Part J — Filtering
print("Delivered Orders:")
df.filter(col("order_status") == "Delivered").show(truncate=False)

print("Delivered Orders with Total Amount > 1000:")
df.filter(
    (col("order_status") == "Delivered") &
    (col("total_amount") > 1000)
).show(truncate=False)

print("Active Customers:")
df.filter(col("is_active") == True).show(truncate=False)

# Part K — Final DataFrame
df = df.select(
    "order_id",
    "customer_id",
    "customer_name",
    "age",
    "restaurant",
    "city",
    "item_price",
    "quantity",
    "delivery_fee",
    "order_status",
    "customer_rating",
    "is_active",
    "food_amount",
    "total_amount"
)

df.show(truncate=False)

# part l - validation
print("Final Schema:")
df.printSchema()

print("Final record count:", df.count())

print("Invalid item prices:")
df.filter(col("item_price") <= 0).show(truncate=False)

print("Invalid quantities:")
df.filter(col("quantity") <= 0).show(truncate=False)

print("Invalid ratings:")
df.filter(col("customer_rating") > 5).show(truncate=False)

print("Calculation verification:")
df.select(
    "order_id",
    "item_price",
    "quantity",
    "delivery_fee",
    "food_amount",
    "total_amount"
).show(truncate=False)

input("Press Enter to stop Spark...")