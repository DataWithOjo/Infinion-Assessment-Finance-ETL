"""
MICROSOFT FABRIC INTEGRATION DEMO
---------------------------------
This script demonstrates how the local Parquet outputs can be ingested 
into a Microsoft Fabric Lakehouse to enable:
1. SQL Analytics Endpoint access.
2. Power BI "Direct Lake" mode.

NOTE: This script is designed to run in a Fabric Notebook environment 
"""

# ==========================================
# STEP 1: INGEST PARQUET TO DELTA
# ==========================================
# In Fabric, we convert standard Parquet files to "Delta Tables".
# This unlocks ACID transactions and the SQL Endpoint.

# from pyspark.sql import SparkSession

# def convert_to_delta(table_name, file_path):
#    
#     # Read the locally generated Parquet file
#     # In Fabric, uploaded files sit in the "Files" section of the Lakehouse
#     source_path = f"Files/data/processed/{file_path}"
#     
#     print(f"Reading {source_path}...")
#     df = spark.read.parquet(source_path)
#     
#     # Write as a Managed Delta Table
#     # This automatically registers it in the Fabric metastore
#     print(f"Saving as Table: {table_name}...")
#     df.write.mode("overwrite").format("delta").saveAsTable(table_name)
#     
#     # Optimize (V-Order)
#     # Fabric's "V-Order" optimization makes Power BI read this 10x faster
#     spark.sql(f"OPTIMIZE {table_name} VORDER")

# ==========================================
# STEP 2: EXECUTION
# ==========================================

# if __name__ == "__main__":
#     # Convert Transaction Fact Table
#     convert_to_delta("fact_transactions", "fact_transactions.parquet")
#     
#     # Convert Loan Fact Table
#     convert_to_delta("fact_loans", "fact_loans.parquet")
    
#     print("Fabric Integration Complete. Tables are now queryable via SQL Endpoint.")