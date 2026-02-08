# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from bronze_functions import *
from bronze_variables import *

# COMMAND ----------

# column mapping config:
config = {
        "folder_name": "5525672 - Hanos - V0", 
        "column_mapping": {
            "wholesaler_name": F.lit("Hanos_5525672"),
            "wholesaler_id": F.lit("5525672"),
            "customer_address": ["unnamed_4","Column5"], 
            "customer_city":["unnamed_6", "Column7"],
            "customer_postal_code": ["unnamed_5","Column6"],
            "customer_country": None,
            "customer_name": ["unnamed_2","Column3"],
            "owner_name": ["unnamed_3","Column4"],
            "customer_number": ["unnamed_1","Column2"],
            "product_number": ["unnamed_7","Column8"],
            "article_name": ["unnamed_11","Column12"],
            "EAN": ["unnamed_21","Column22"],
            "total_amount": ["unnamed_9","Column10"],
            "UOM": ["unnamed_8","Column9"],
            "vat_number": None,
            "date": "full_file_path",
            "full_file_path": F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace("full_file_path", "dbfs:", ""), "%20", " "), "%3E", ">"), " ", "_")
        }
}

# COMMAND ----------

# static wholesaler config:
wholesaler_name = "Hanos_5525672_V0"
base_file_path = "/Volumes/d_dnst_000_landing/volumes/sellout_data/5525672___Hanos___V0/"

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
