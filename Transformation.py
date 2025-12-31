# Databricks notebook source
# Customers who have ordered more than one item
from pyspark.sql import functions as F

# Load tables
customers = spark.table("customer_2020")
orders = spark.table("orders_2020")

# Join, aggregate, and apply HAVING filter
result_df = (
    customers.alias("c")
    .join(
        orders.alias("o"),
        F.col("c.CustomerKey") == F.col("o.CustomerKey"),
        "inner"
    )
    .groupBy(
        F.col("c.CustomerKey"),
        F.col("c.Firstname"),
        F.col("c.Lastname")
    )
    .agg(
        F.sum(F.col("o.OrderQuantity")).alias("TotalOrderQuantity")
    )
    .filter(F.col("TotalOrderQuantity") > 1)  # HAVING clause
)

result_df.display()

result_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.curated_TotalOrderQuantity")
