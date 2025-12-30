# Databricks notebook source
Product_Category_csv_file_path = "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/Databricks_Project/Databricks_Project/Product_Category.csv"

df = spark.read.format("csv") \
    .option("header", "true").option("inferSchema", "true").load(Product_Category_csv_file_path)

display(df)

table_name = "Product_Category"
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Product_Category")

print(f"Table '{table_name}' created successfully!")
