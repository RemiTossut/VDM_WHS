# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from bronze_functions import *
from bronze_variables import *

# COMMAND ----------

# column mapping config:
config = {
        "folder_name": "5526091 - Spuntini Deerlijk - V0", 
        "column_mapping": {
            "wholesaler_name": F.lit("Spuntini_Deerlijk_5526091"),
            "wholesaler_id": F.lit("5526091"),
            "customer_address": "sell_to_address", 
            "customer_city": "sell_to_city", 
            # "customer_postal_code": F.regexp_extract(F.col("sell_to_post_code"), r'-(\S+)', 1),
            "customer_postal_code": F.regexp_replace(F.col("sell_to_post_code"), r'^.*-', ''),
            "customer_country": F.lit("BE"),
            "customer_name": "sell_to_customer_name",
            "owner_name": None,
            "customer_number": "sell_to_customer_no_",
            "date": "date",
            "product_number": "vendor_item_no_",
            "article_name": "description",
            "EAN": None,
            "total_amount": "quantity",
            "UOM":"base_unit_of_measure",
            "vat_number": "enterprise_no",
            "date": "date",
            "full_file_path": F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace("full_file_path", "dbfs:", ""), "%20", " "), "%3E", ">"), " ", "_")
        }
}

# COMMAND ----------

# static wholesaler config:
wholesaler_name = "Spuntini_Deerlijk_5526091_V0"
base_file_path = "/Volumes/d_dnst_000_landing/volumes/sellout_data/5526091___Spuntini_Deerlijk___V0/"

# COMMAND ----------

#dynamic wholesaler config
target_table = wholesaler_name.lower()
dependency_table_path = f"d_dnst_000_landing.metadata.{target_table}_metadata"

# COMMAND ----------

# general config:
base_volume_path= "/Volumes/d_dnst_000_landing/volumes/sellout_data"
bronze_base_table_path = "/Volumes/d_dnst_010_bronze"
target_catalog = "d_dnst_010_bronze"
target_schema = "sharepoint"

# COMMAND ----------

def get_fn_flowbase():
    return process_wholesaler_csv_files(spark, wholesaler_name, config, base_volume_path, bronze_base_table_path, golden_template_schema)

# COMMAND ----------

fn_flowbase = get_fn_flowbase
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name": target_table,
            "operation_type": "overwrite",
            "dependencies": [
               dependency_table_path
               ],
            "table_structure":  create_spark_schema_from_dict(golden_template_schema)
            }
