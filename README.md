# Project: Data Engineering Exercise

#Goal:
Build a small end-to-end data pipeline that ingests raw datasets from a source system, transforms them into a simple analytical tabular data model, and produces basic insights for analytics and reporting.

Source Data: The following source files are uploaded to the Databricks Volume "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/"
Source Files:
- customer_2020.csv
- orders_2020.csv
- territory.csv

Pipeline Architecture
The pipeline follows a layered data architecture:
Raw Zone (Bronze Layer):
- Stores raw ingested data as-is from source files
- Ensures traceability and supports reprocessing

Silver Zone:
- Contains transformed, cleaned, and enriched datasets
- Optimized for analytical queries

Insights:
- Business insights derived from Silver layer tables

Step 1: Ingestion to Raw Zone
Script Name: Ingestion_to_raw_zone
This script ingests CSV files from the Databricks Volume into raw tables:
- customer_2020
- orders_2020
- territory

Step 2: Transformation1 - Customers who have ordered more than one item
Script Name: Transformation1_Cust_Order
- Joins customer and order data to create a Silver layer table.
- Insight: Customers who have ordered more than one item
- Target Table: workspace.transformation_layer.curated_tota_order_quantity

Step 3: Transformation – Products per Territory
Script Name: Transformation2_Prod_per_Territory
Transforms order and territory data.
Logic:
- Exclude Australia
- Rename North America to Americas
- Insight: Number of products ordered per teritory
- Target Table: workspace.transformation_layer.curated_sales_by_territory









