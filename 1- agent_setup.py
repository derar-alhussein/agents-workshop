# Databricks notebook source
import requests
import pandas as pd
import io

# Initialize clients
w = WorkspaceClient()
client = VectorSearchClient()

base_url = "https://raw.githubusercontent.com/derar-alhussein/agents-workshop/data"
csv_files = {
    "cust_service_data": f"{base_url}/cust_service_data.csv",
    "policies": f"{base_url}/policies.csv", 
    "product_docs": f"{base_url}/product_docs.csv"
} 

# Create catalog if not exists
spark.sql("CREATE CATALOG IF NOT EXISTS agents_lab")

# Create schema if not exists
spark.sql("CREATE SCHEMA IF NOT EXISTS agents_lab.product")

# Download and load each CSV file
for table_name, url in csv_files.items():
    # Download CSV data
    response = requests.get(url)
    response.raise_for_status()
    
    # Read CSV into pandas DataFrame
    df = pd.read_csv(io.StringIO(response.text))
    
    # Convert to Spark DataFrame and write to table
    spark_df = spark.createDataFrame(df)
    spark_df.write.mode("overwrite").saveAsTable(f"agents_lab.product.{table_name}")

print("Tables created successfully")
