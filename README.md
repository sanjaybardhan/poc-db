# Project: Data Engineering Exercise

Goal: Build a small data pipeline that ingests raw datasets from a source system, transforms them into a simple analytical tabular data model and produces one or two basic insights.

Source file: Following source files uploaded in Databricks volume "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/"
             customer_2020.csv
             orderss_2020.csv
             territory.csv
               
Ingestion_to_raw_zone: This script is to ingest csv file from volume, "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/ to respective tables in raw_zone.
             customer_2020
             orders_2020
             territory

Transformation1_Cust_Order: This will transform "customer_2020" & "orderss_2020" tables to create a table in Silver layer and produce insight "Customers who have ordered more than one item"

Transformation2_Prod_per_Territory: This will transform "orders_2020" & "territory" tables to create a table in Silver layer and produce insight "Number of products ordered per territory where the country is not equal to Australia.   If the continent is equal to North America, then display Americas". 
