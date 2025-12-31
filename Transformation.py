# 1. Insights: Customers who have ordered more than one item
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

# COMMAND ----------

# 2.Insigntes: Number of products ordered per territory where the country is not equal to Australia. If the continent is equal to North America, then display Americas  Concatinate region + country (Area).   

from pyspark.sql import functions as F

# Load tables
orders = spark.table("orders_2020")
territory = spark.table("territory")

result_df = (
    orders.alias("o")
    .join(
        territory.alias("t"),
        F.col("o.territorykey") == F.col("t.SalesTerritoryKey"),
        "inner"
    )
    .filter(F.col("t.country") != "Australia")  # WHERE clause
    .groupBy(
        F.col("o.territorykey"),
        F.col("t.region"),
        F.col("t.country"),
        F.col("t.continent")
    )
    .agg(
        F.count(F.col("o.productkey")).alias("product_count")
    )
    .withColumn(
        "continent",
        F.when(
            F.col("t.continent") == "North America",
            F.lit("Americas")
        ).otherwise(F.col("t.continent"))
    )
    .withColumn(
        "Area",
        F.concat(
            F.col("t.region"),
            F.col("t.country")
        )
    )
    .select(
        F.col("territorykey"),
        F.col("continent"),
        F.col("Area"),
        F.col("product_count")
    )
)

result_df.display()

result_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.curated_sales_by_territory")


