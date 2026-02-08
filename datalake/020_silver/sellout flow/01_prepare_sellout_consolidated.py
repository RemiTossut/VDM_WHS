# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

# MAGIC %md
# MAGIC The goal of this notebook is to take all separete tables (one per wholesaler, already in the same schema) and combine them into one large table

# COMMAND ----------

from imports import *

# COMMAND ----------

target_catalog = "d_dnst_020_silver"
target_schema = "silver_01"
target_table = "01_sellout_consolidated"

bronze_base_table_path = "d_dnst_010_bronze.sharepoint"

# COMMAND ----------

# %sql
# DROP TABLE d_dnst_020_silver.silver_01.01_sellout_consolidated

# COMMAND ----------

tables = spark.sql(f"SHOW TABLES IN {bronze_base_table_path}").collect()

dependencies_dict = {
    row.tableName: f"{bronze_base_table_path}.{row.tableName}"
    for row in tables
}

# COMMAND ----------

def create_unionized_dataframe() -> DataFrame:
    """
    Reads all individual bronze Delta tables for each wholesaler and unions them into a single DataFrame.
    It assumes the individual tables already conform to a consistent schema.

    Returns:
        DataFrame: A single Spark DataFrame containing unionized data from all wholesalers.
                   Returns an empty DataFrame if no tables are found or an error occurs.
                   The schema of the returned DataFrame will be the union of all schemas,
                   handled by unionByName(allowMissingColumns=True).
    """
    print(f"\n--- Unionizing all bronze tables from {bronze_base_table_path} ---")

    customer_configs = dependencies_dict 
    all_wholesaler_dfs = []
    
    # Placeholder for an empty DataFrame if no tables are found
    # Using a minimal schema for the initial empty DF, as we are not enforcing the full golden schema here.
    empty_df_schema = StructType([StructField("wholesaler_name", StringType(), True)])
    
    for wholesaler_name in customer_configs.keys():
        table_name = f"{bronze_base_table_path}.{wholesaler_name.lower()}"
        print(f"Attempting to read table: {table_name}")
        try:
            # Read the Delta table directly
            df_wholesaler = spark.read.format("delta").table(table_name)
            
            if df_wholesaler.isEmpty():
                print(f"Table {table_name} is empty. Skipping from union.")
                continue

            print(f"Successfully read {table_name} with {df_wholesaler.count()} rows.")
            all_wholesaler_dfs.append(df_wholesaler)

        except Exception as e:
            print(f"Warning: Could not read table {table_name}: {e}. Skipping this table.")
            # import traceback; traceback.print_exc() # Uncomment for detailed error logging

    if not all_wholesaler_dfs:
        print("No DataFrames were successfully read to union. Returning empty DataFrame.")
        # Return an empty DataFrame, its schema will be based on the first DataFrame if any,
        # otherwise a minimal one.
        return spark.createDataFrame([], schema=empty_df_schema)
        
    # Perform the unionByName operation
    # allowMissingColumns=True is crucial here as we are not explicitly aligning schemas
    unionized_df = all_wholesaler_dfs[0]
    for i in range(1, len(all_wholesaler_dfs)):
        unionized_df = unionized_df.unionByName(all_wholesaler_dfs[i], allowMissingColumns=True) 

    print(f"Successfully unionized {len(all_wholesaler_dfs)} DataFrames. Total unionized rows: {unionized_df.count()}")
    return unionized_df


# COMMAND ----------

enable_columnmapping_sql_code = f"""
ALTER TABLE {target_catalog}.{target_schema}.{target_table} 
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
"""

# COMMAND ----------

fn_flowbase = create_unionized_dataframe
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name":target_table,
            "operation_type": "overwrite",
            "table_structure": (
               "wholesaler_id STRING, "
               "customer_address STRING, "
               "customer_city STRING, "
               "customer_postal_code STRING, "
               "customer_country STRING, "
               "customer_name STRING, "
               "customer_number STRING, "
               "product_number STRING, "
               "article_name STRING, "
               "EAN STRING, "
               "total_amount DOUBLE, "
               "UOM STRING, "
               "wholesaler_name STRING, "
               "vat_number STRING, "
               "date STRING, "
               "full_file_path STRING, "
               "owner_name STRING, "
               "run_id BIGINT"
            ),
            "dependencies": 
               list(dependencies_dict.values()),
            "operation_hooks": [
                {
                    "id": "set columnMapping true",
                    "code_language": "sql",
                    "trigger_timing": "pre",
                    "trigger_functions": ["execute", "execute_full"], 
                    "code": enable_columnmapping_sql_code
                }
            ]
            }
