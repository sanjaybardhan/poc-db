# Databricks notebook source
# Define Source path
Product_Category_csv_file_path = "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/Databricks_Project/Databricks_Project/Product_Category.csv"

# Read file from source path, automatically determine column data types
df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true").load(Product_Category_csv_file_path)

display(df)

# Write to a Table, mode: change to Append for incremental loads
table_name = "Product_Category"
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Product_Category")

print(f"Table '{table_name}' created successfully!")

# COMMAND ----------

# Define Source path
Product_Subcategory_csv_file_path = "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/Databricks_Project/Databricks_Project/Product_Subcategory.csv"

# Read file from source path, automatically determine column data types
df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true").load(Product_Subcategory_csv_file_path)

display(df)

# Write to a Table, mode: change to Append for incremental loads
table_name = "Product_Subcategory"
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Product_Subcategory")

print(f"Table '{table_name}' created successfully!")

# COMMAND ----------

# Define Source path
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
    .saveAsTable("Territory")
print(f"Table '{table_name}' created successfully!")

# COMMAND ----------

# Define Source path
Product_2020_csv_file_path = "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/Databricks_Project/Databricks_Project/Product_2020.csv"

# Read file from source path, # automatically determine column data types
df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true").load(Product_2020_csv_file_path)

display(df)

# Write to a Table, mode: change to Append for incremental loads
table_name = "Product_2020"
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Product_2020")
print(f"Table '{table_name}' created successfully!")

# COMMAND ----------

# Define Source path
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
    .saveAsTable("Customer_2020")

print(f"Table '{table_name}' created successfully!")

# COMMAND ----------

# Define Source path
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
    .saveAsTable("Orders_2020")

print(f"Table '{table_name}' created successfully!")
