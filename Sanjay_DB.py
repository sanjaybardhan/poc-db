# Databricks notebook source
Product_Category_csv_file_path = "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/Databricks_Project/Databricks_Project/Product_Category.csv"

df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true").load(Product_Category_csv_file_path).withColumnRenamed("ProductCategoryKey", "ProductCategoryKey_Unique")

display(df)
table_name = "Product_Category"

df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("Product_Category")



df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true").load(Product_Category_csv_file_path)

# COMMAND ----------

Product_Subcategory_csv_file_path = "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/Databricks_Project/Databricks_Project/Product_Subcategory.csv"

df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true").load(Product_Subcategory_csv_file_path)

display(df)

table_name = "Product_Subcategory_csv_file_path"

df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("Product_Subcategory")


print(f"Table '{Product_Subcategory}' created successfully!")

# COMMAND ----------

Territory_csv_file_path = "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/Databricks_Project/Databricks_Project/Territory.csv"

df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true").load(Territory_csv_file_path)

display(df)

table_name = "Territory"

df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("Territory")


print(f"Table '{Territory}' created successfully!")

# COMMAND ----------

Product_2020_csv_file_path = "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/Databricks_Project/Databricks_Project/Product_2020.csv"

df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true").load(Product_2020_csv_file_path)

display(df)

table_name = "Product_2020"

df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("Product_2020")


print(f"Table '{Product_2020}' created successfully!")

# COMMAND ----------

Customer_2020_csv_file_path = "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/Databricks_Project/Databricks_Project/Customer_2020.csv"

df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true").load(Customer_2020_csv_file_path)

display(df)

table_name = "Customer_2020"

df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("Customer_2020")
print(f"Table '{Customer_2020}' created successfully!")

# COMMAND ----------

Orders_2020_csv_file_path = "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/Databricks_Project/Databricks_Project/Orders_2020.csv"

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true").load(Orders_2020_csv_file_path)

display(df)

table_name = "Orders_2020"

df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("Orders_2020")

print(f"Table '{Orders_2020}' created successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.customer_2020

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_product_orders AS (
# MAGIC     SELECT
# MAGIC         o.ProductKey,
# MAGIC         o.CustomerKey,
# MAGIC         COUNT(DISTINCT o.OrderNumber) AS TotalOrders
# MAGIC     FROM orders_2020 o
# MAGIC     GROUP BY o.ProductKey, o.CustomerKey
# MAGIC ),
# MAGIC ranked AS (
# MAGIC     SELECT
# MAGIC         cpo.*,
# MAGIC         RANK() OVER (PARTITION BY ProductKey ORDER BY TotalOrders DESC) AS rnk
# MAGIC     FROM customer_product_orders cpo
# MAGIC )
# MAGIC SELECT
# MAGIC     r.ProductKey,
# MAGIC     r.CustomerKey,
# MAGIC     r.TotalOrders
# MAGIC FROM ranked r
# MAGIC WHERE r.rnk = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table temp_customer AS
# MAGIC WITH customer_product_orders AS (
# MAGIC     SELECT
# MAGIC         o.ProductKey,
# MAGIC         o.CustomerKey,
# MAGIC         COUNT(DISTINCT o.OrderNumber) AS TotalOrders
# MAGIC     FROM Orders_2020 o
# MAGIC     GROUP BY o.ProductKey, o.CustomerKey
# MAGIC ),
# MAGIC ranked AS (
# MAGIC     SELECT
# MAGIC         cpo.*,
# MAGIC         RANK() OVER (
# MAGIC             PARTITION BY cpo.ProductKey
# MAGIC             ORDER BY cpo.TotalOrders DESC
# MAGIC         ) AS rnk
# MAGIC     FROM customer_product_orders cpo
# MAGIC )
# MAGIC SELECT
# MAGIC     r.ProductKey,
# MAGIC     p.ProductName,
# MAGIC     r.CustomerKey,
# MAGIC     CONCAT(c.Prefix, ' ', c.FirstName, ' ', c.LastName) AS CustomerName,
# MAGIC     r.TotalOrders
# MAGIC FROM ranked r
# MAGIC INNER JOIN Customer_2020 c
# MAGIC     ON r.CustomerKey = c.CustomerKey
# MAGIC INNER JOIN Product_2020 p
# MAGIC     ON r.ProductKey = p.ProductKey
# MAGIC WHERE r.rnk = 1
# MAGIC ORDER BY r.ProductKey;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH SubcategoryMaxSales AS (
# MAGIC   SELECT
# MAGIC     p.ProductSubcategoryKey,
# MAGIC     MAX(o.OrderQuantity * p.ProductPrice) AS MaxSaleBySubcategory
# MAGIC   FROM orders_2020 o
# MAGIC   INNER JOIN Product_2020 p ON o.ProductKey = p.ProductKey
# MAGIC   GROUP BY p.ProductSubcategoryKey
# MAGIC )
# MAGIC SELECT
# MAGIC   c.CustomerKey,
# MAGIC   CONCAT(c.Prefix, ' ', c.FirstName, ' ', c.LastName) AS FullName,
# MAGIC   COUNT(DISTINCT o.OrderNumber) AS TotalOrders,
# MAGIC   SUM(o.OrderQuantity) AS TotalQuantity,
# MAGIC   MAX(o.OrderQuantity * p.ProductPrice) AS MaxSaleByProduct,
# MAGIC   s.MaxSaleBySubcategory
# MAGIC FROM orders_2020 o
# MAGIC INNER JOIN Customer_2020 c ON o.CustomerKey = c.CustomerKey
# MAGIC INNER JOIN Product_2020 p ON o.ProductKey = p.ProductKey
# MAGIC LEFT JOIN SubcategoryMaxSales s ON p.ProductSubcategoryKey = s.ProductSubcategoryKey
# MAGIC GROUP BY
# MAGIC   c.CustomerKey, c.Prefix, c.FirstName, c.LastName, s.MaxSaleBySubcategory

# COMMAND ----------

from pyspark.sql import functions as F
orders = spark.table("orders_2020")
products = spark.table("Product_2020")
customers = spark.table("Customer_2020")

# -----------------------------------
# CTE: SubcategoryMaxSales
# -----------------------------------
subcategory_max_sales = (
    orders.alias("o")
    .join(products.alias("p"), F.col("o.ProductKey") == F.col("p.ProductKey"), "inner")
    .groupBy(F.col("p.ProductSubcategoryKey"))
    .agg(
        F.max(F.col("o.OrderQuantity") * F.col("p.ProductPrice"))
        .alias("MaxSaleBySubcategory")
    )
)

# -----------------------------------
# Main Query
# -----------------------------------
result_df = (
    orders.alias("o")
    .join(customers.alias("c"), F.col("o.CustomerKey") == F.col("c.CustomerKey"), "inner")
    .join(products.alias("p"), F.col("o.ProductKey") == F.col("p.ProductKey"), "inner")
    .join(
        subcategory_max_sales.alias("s"),
        F.col("p.ProductSubcategoryKey") == F.col("s.ProductSubcategoryKey"),
        "left"
    )
    .groupBy(
        F.col("c.CustomerKey"),
        F.col("c.Prefix"),
        F.col("c.FirstName"),
        F.col("c.LastName"),
        F.col("s.MaxSaleBySubcategory")
    )
    .agg(
        F.countDistinct(F.col("o.OrderNumber")).alias("TotalOrders"),
        F.sum(F.col("o.OrderQuantity")).alias("TotalQuantity"),
        F.max(F.col("o.OrderQuantity") * F.col("p.ProductPrice"))
            .alias("MaxSaleByProduct")
    )
    .withColumn(
        "FullName",
        F.concat_ws(
            " ",
            F.col("c.Prefix"),
            F.col("c.FirstName"),
            F.col("c.LastName")
        )
    )
)

result_df.display()  # Databricks-friendly


# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table temp_customer AS
# MAGIC WITH customer_product_qty AS (
# MAGIC     SELECT
# MAGIC         o.CustomerKey,
# MAGIC         o.ProductKey,
# MAGIC         SUM(o.OrderQuantity) AS TotalQuantity
# MAGIC     FROM Orders_2020 o
# MAGIC     GROUP BY o.CustomerKey, o.ProductKey
# MAGIC ),
# MAGIC ranked AS (
# MAGIC     SELECT
# MAGIC         cpq.*,
# MAGIC         RANK() OVER (
# MAGIC             PARTITION BY cpq.ProductKey
# MAGIC             ORDER BY cpq.TotalQuantity DESC
# MAGIC         ) AS rnk
# MAGIC     FROM customer_product_qty cpq
# MAGIC )
# MAGIC SELECT
# MAGIC     r.ProductKey,
# MAGIC     p.ProductName,
# MAGIC     r.CustomerKey,
# MAGIC     CONCAT(c.Prefix, ' ', c.FirstName, ' ', c.LastName) AS CustomerName,
# MAGIC     r.TotalQuantity AS MaxQuantityOrdered
# MAGIC FROM ranked r
# MAGIC INNER JOIN Customer_2020 c
# MAGIC     ON r.CustomerKey = c.CustomerKey
# MAGIC INNER JOIN product_2020 p
# MAGIC     ON r.ProductKey = p.ProductKey
# MAGIC WHERE r.rnk = 1
# MAGIC ORDER BY r.ProductKey;
# MAGIC

# COMMAND ----------

select * from temp_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select c.CustomerKey, c.Firstname, c.Lastname, sum(o.OrderQuantity)
# MAGIC From customer_2020 c
# MAGIC inner join orders_2020 o
# MAGIC on c.CustomerKey = o.CustomerKey
# MAGIC group by c.CustomerKey, c.Firstname, c.Lastname
# MAGIC having sum(o.OrderQuantity) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC update orders_2020
# MAGIC SET orderquantity = 12 where ordernumber in ('SO45136', 'SO45135', 'SO45145', 'SO45140')
# MAGIC
# MAGIC update orders_2020
# MAGIC SET customerkey = 29143 where ordernumber in ('SO45136', 'SO45135', 'SO45145', 'SO45140')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC update orders_2020
# MAGIC SET customerkey = 29143 where ordernumber in ('SO45136', 'SO45135', 'SO45145', 'SO45140')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_2020

# COMMAND ----------

# MAGIC %sql
# MAGIC select c.CustomerKey, c.Firstname, c.Lastname, sum(o.OrderQuantity)
# MAGIC From customer_2020 c
# MAGIC inner join orders_2020 o
# MAGIC on c.CustomerKey = o.CustomerKey
# MAGIC group by c.CustomerKey, c.Firstname, c.Lastname
# MAGIC having sum(o.OrderQuantity) > 1

# COMMAND ----------

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


# COMMAND ----------

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

# COMMAND ----------

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


# COMMAND ----------

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
    .saveAsTable("workspace.default.sales_by_territory")

# COMMAND ----------

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

# COMMAND ----------

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

##
