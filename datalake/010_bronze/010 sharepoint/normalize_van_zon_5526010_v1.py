# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from bronze_functions import *
from bronze_variables import *

# COMMAND ----------

# column mapping config:
config = {
        "folder_name": "5526010 - Van Zon - V1", 
        "column_mapping": {
            "wholesaler_name": F.lit("Van_Zon_5526010"),
            "wholesaler_id": F.lit("5526010"),
            "customer_address": "klantadres",
            "customer_city": "klantgemeente",
            "customer_postal_code": "klantpostcode", 
            "customer_country": None, 
            "customer_name": "klant",
            "owner_name": None,
            "customer_number": "klantnr",
            "product_number": "artikelnr__lev_",
            "article_name": "artikel",
            "EAN": None,
            "total_amount": "verkoopaantallen",
            "UOM": "eenheid",
            "vat_number": "customerondernemingsnr",
            "date": "full_file_path",
            "full_file_path": F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace("full_file_path", "dbfs:", ""), "%20", " "), "%3E", ">"), " ", "_")
        },
        # "csv_read_config": { # NEW: Configuration for reading the staged CSVs with pandas
        #     "header": 21, # 0-indexed. This means the 20th row is the header.
        #     "sep": ",",
        #     "encoding": "latin1", # Common for European characters
        #     "skip_blank_lines": True,
        #     "on_bad_lines": "skip", # 'warn', 'skip', or a callable
        # }
}

# COMMAND ----------

# static wholesaler config:
wholesaler_name = "Van_Zon_5526010_V1"
base_file_path = "/Volumes/d_dnst_000_landing/volumes/sellout_data/5526010___Van_Zon___V1/"

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
