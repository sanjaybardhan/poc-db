# Project: Data Engineering Exercise

Goal: Build a small data pipeline that ingests raw datasets from a source system, transforms them into a simple analytical tabular data model and produces one or two basic insights.

Sanjay.DB: This script is to ingest ingests raw datasets(csv file)  from volume, "/Volumes/workspace/sanjay_db_schema/sanjay_db_volume/ to respective tables"

Transformation1_Cust_Order: This will transform "customer_2020" & "orderss_2020" to create a table and produce insight "Customers who have ordered more than one item"

Transformation2_Prod_per_Territory: This will transform "orders_2020" & "territory" to create a table and produce insight "Number of products ordered per territory where the country is not equal to Australia.   If the continent is equal to North America, then display Americas. 
