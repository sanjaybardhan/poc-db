# Databricks notebook source
# Ingestion of Territory_csv
Territory_csv_file_path = "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/Databricks_Project/Databricks_Project/Territory.csv"

# Read file from source path, automatically determine column data types
df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true").load(Territory_csv_file_path)

display(df)

# Write to a Table, mode: change to Append for incremental loads
table_name = "Territory"
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.raw_zone.Territory")
print(f"Table '{table_name}' created successfully!")

# COMMAND ----------

# Ingestion of Customer_2020_csv
Customer_2020_csv_file_path = "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/Databricks_Project/Databricks_Project/Customer_2020.csv"

# Read file from source path, automatically determine column data types
df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true").load(Customer_2020_csv_file_path)

display(df)

# Write to a Table, mode: change to Append for incremental loads
table_name = "Customer_2020"
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.raw_zone.Customer_2020")

print(f"Table '{table_name}' created successfully!")

# COMMAND ----------

# Ingestion of Orders_2020_csv
Orders_2020_csv_file_path = "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/Databricks_Project/Databricks_Project/Orders_2020.csv"

# Read file from source path, automatically determine column data types
df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true").load(Orders_2020_csv_file_path)

display(df)

# Write to a Table, mode: change to Append for incremental loads
table_name = "Orders_2020"
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.raw_zone.Orders_2020")

print(f"Table '{table_name}' created successfully!")
