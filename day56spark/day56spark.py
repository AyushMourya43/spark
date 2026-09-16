# CLASS 6 – HOMEWORK ASSIGNMENT
# FOOD DELIVERY ANALYTICS

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    avg,
    min,
    max,
    count
)

spark = ( 
    SparkSession.builder 
    .appName("Class6_Food_Delivery_Analytics") 
    .master("local[*]")
    .getOrCreate()
)

# ============================================================
# 2. CUSTOMERS DATA
# ============================================================

customers_data = [
    (501, "Rahul", "Delhi"),
    (502, "Priya", "Mumbai"),
    (503, "Aman", "Bangalore"),
    (504, "Sneha", "Hyderabad"),
    (505, "Karan", "Delhi"),
    (506, "Neha", "Mumbai"),
    (507, "Riya", "Bangalore"),
    (508, "Vikram", "Pune"),
    (509, "Ankit", "Hyderabad"),
    (510, "Pooja", "Pune"),
    (511, "Nikhil", "Delhi"),
    (512, "Simran", "Bangalore")
]

customers_columns = [
    "customer_id",
    "customer_name",
    "city"
]

customers_df = spark.createDataFrame(
    customers_data,
    customers_columns
)


# ============================================================
# 3. RESTAURANTS DATA
# ============================================================

restaurants_data = [
    (601, "Spice Garden", "Indian", "Delhi"),
    (602, "Pizza Hub", "Italian", "Mumbai"),
    (603, "Dragon Bowl", "Chinese", "Bangalore"),
    (604, "Burger Point", "Fast Food", "Hyderabad"),
    (605, "South Kitchen", "South Indian", "Bangalore"),
    (606, "Curry House", "Indian", "Pune"),
    (607, "Pasta Street", "Italian", "Delhi"),
    (608, "Taco Town", "Mexican", "Mumbai")
]

restaurants_columns = [
    "restaurant_id",
    "restaurant_name",
    "cuisine",
    "restaurant_city"
]

restaurants_df = spark.createDataFrame(
    restaurants_data,
    restaurants_columns
)


# ============================================================
# 4. ORDERS DATA
# ============================================================

orders_data = [
    (7001, 501, 601, 2, 420, 32, "Delivered", "2026-08-01"),
    (7002, 502, 602, 1, 650, 41, "Delivered", "2026-08-01"),
    (7003, 503, 603, 3, 780, 28, "Delivered", "2026-08-02"),
    (7004, 504, 604, 2, 520, 55, "Delivered", "2026-08-02"),
    (7005, 505, 601, 1, 230, 36, "Cancelled", "2026-08-03"),
    (7006, 506, 605, 3, 450, 25, "Delivered", "2026-08-03"),
    (7007, 507, 603, 2, 540, 31, "Delivered", "2026-08-04"),
    (7008, 508, 606, 4, 920, 47, "Delivered", "2026-08-04"),
    (7009, 509, 604, 1, 260, 62, "Delivered", "2026-08-05"),
    (7010, 510, 608, 2, 680, 38, "Delivered", "2026-08-05"),
    (7011, 511, 607, 2, 590, 44, "Delivered", "2026-08-06"),
    (7012, 512, 605, 1, 160, 22, "Delivered", "2026-08-06"),
    (7013, 501, 602, 2, 1300, 35, "Delivered", "2026-08-07"),
    (7014, 502, 608, 3, 1020, 52, "Delivered", "2026-08-07"),
    (7015, 503, 601, 2, 460, 29, "Delivered", "2026-08-08"),
    (7016, 504, 603, 1, 260, 34, "Cancelled", "2026-08-08"),
    (7017, 505, 606, 2, 460, 48, "Delivered", "2026-08-09"),
    (7018, 506, 604, 3, 780, 59, "Delivered", "2026-08-09"),
    (7019, 507, 607, 1, 295, 39, "Delivered", "2026-08-10"),
    (7020, 508, 605, 4, 640, 27, "Delivered", "2026-08-10"),
    (7021, 509, 601, 2, 440, 33, "Delivered", "2026-08-11"),
    (7022, 510, 602, 1, 650, 46, "Delivered", "2026-08-11"),
    (7023, 511, 603, 2, 520, 30, "Delivered", "2026-08-12"),
    (7024, 512, 608, 2, 680, 43, "Delivered", "2026-08-12"),
    (7025, 501, 606, 3, 690, 51, "Delivered", "2026-08-13"),
    (7026, 502, 605, 2, 320, 26, "Delivered", "2026-08-13"),
    (7027, 503, 604, 4, 1040, 64, "Delivered", "2026-08-14"),
    (7028, 504, 601, 1, 210, 37, "Delivered", "2026-08-14"),
    (7029, 505, 607, 2, 590, 42, "Delivered", "2026-08-15"),
    (7030, 506, 602, 2, 1300, 49, "Delivered", "2026-08-15")
]

orders_columns = [
    "order_id",
    "customer_id",
    "restaurant_id",
    "items",
    "order_amount",
    "delivery_time",
    "status",
    "order_date"
]

orders_df = spark.createDataFrame(
    orders_data,
    orders_columns
)

# 1. Find the total number of orders in the dataset.
orders_df.count()
print("Total orders:", orders_df.count())

# 2. Find the total number of food items ordered.
orders_df.select(
    sum("items").alias("total_items")
).show()

# 3. Find the total revenue generated from all orders.
orders_df.select(
    sum("order_amount").alias("total_revenue")
).show()

# 4. Find the average order amount.
orders_df.select(
    avg("order_amount").alias("average_order_amount")
).show()

# 5. Find the minimum and maximum order amount.
orders_df.select(
    min("order_amount").alias("minimum_order_amount"),
    max("order_amount").alias("maximum_order_amount")
).show()

# 6. Find the number of orders placed by each customer.
orders_df.groupBy("customer_id").count().show()

# 7. Find the total amount spent by each customer.
orders_df.groupBy("customer_id").agg(
    sum("order_amount").alias("total_spent")
).show()

# 8. Find the total number of orders received by each restaurant.
orders_df.groupBy("restaurant_id").count().show()

# 9. Find the total revenue generated by each restaurant.
orders_df.groupBy("restaurant_id").agg(
    sum("order_amount").alias("total_revenue")
).show()

# 10. For each restaurant, calculate the total orders, total revenue, and average order amount.
orders_df.groupBy("restaurant_id").agg(
    count("order_id").alias("total_orders"),
    sum("order_amount").alias("total_revenue"),
    avg("order_amount").alias("average_order_amount")
).show()

# 11. Find the top 5 customers based on their total spending.
orders_df.groupBy("customer_id").agg(
    sum("order_amount").alias("total_spent")
).orderBy("total_spent", ascending=False).limit(5).show()

# 12. Find the top 5 restaurants based on total revenue.
orders_df.groupBy("restaurant_id").agg(
    sum("order_amount").alias("total_revenue")
).orderBy("total_revenue", ascending=False).limit(5).show()

# 13. Find the total revenue generated by each city and sort it from highest to lowest.
orders_df.join(
    customers_df,
    orders_df.customer_id == customers_df.customer_id
).groupBy("city").agg(
    sum("order_amount").alias("total_revenue")
).orderBy(
    "total_revenue",
    ascending=False
).show()

# 14. Find the average delivery time for each restaurant.

orders_df.groupBy("restaurant_id").agg(
    avg("delivery_time").alias("average_delivery_time")
).show()

# 15. Find the total revenue generated from only "Delivered" orders.

orders_df.filter(
    col("status") == "Delivered"
).select(
    sum("order_amount").alias("delivered_revenue")
).show()

# 16. Find the number of "Cancelled" orders.
orders_df.filter(
    col("status") == "Cancelled"
).count()

# 17. Using Spark SQL, find the total revenue generated by each cuisine and sort the result from highest to lowest.

# Spark SQL
orders_df.createOrReplaceTempView("orders")
restaurants_df.createOrReplaceTempView("restaurants")


spark.sql("""
    SELECT
        r.cuisine,
        SUM(o.order_amount) AS total_revenue
    FROM orders o
    JOIN restaurants r
        ON o.restaurant_id = r.restaurant_id
    GROUP BY r.cuisine
    ORDER BY total_revenue DESC
""").show()

# 18. Using Spark SQL, create a report containing:
#     restaurant_name,
#     cuisine,
#     total_orders,
#     total_revenue,
#     average_order_amount,
#     average_delivery_time

#     Sort the final result by total_revenue in descending order.

spark.sql("""
    SELECT
        r.restaurant_name,
        r.cuisine,
        COUNT(o.order_id) AS total_orders,
        SUM(o.order_amount) AS total_revenue,
        AVG(o.order_amount) AS average_order_amount,
        AVG(o.delivery_time) AS average_delivery_time
    FROM orders o
    JOIN restaurants r
        ON o.restaurant_id = r.restaurant_id
    GROUP BY
        r.restaurant_name,
        r.cuisine
    ORDER BY total_revenue DESC
""").show()

input("Press Enter to stop Spark...")