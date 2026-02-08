# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from bronze_functions import *
from bronze_variables import *

# COMMAND ----------

# column mapping config:
config = {
        "folder_name": "5526040 - Spuntini Degroof - V0", 
        "file_pattern": "*.xlsx",
        "column_mapping": {
            "wholesaler_name": F.lit("Spuntini_Degroof_5526040"),
            "wholesaler_id": F.lit("5526040"),
            "customer_address": ["adres"], 
            "customer_city": ["kl_woonplaats"], 
            "customer_postal_code": ["postcode"],
            "customer_country": F.lit("BE"),
            "customer_name": ["klant","001_degroof_spuntini_nv"],
            "owner_name": None,
            "customer_number": ["klantnummer"],
            "product_number": "artikelcode_leverancier",
            "article_name": ["artikel"],
            "EAN": None,
            "total_amount": ["hoeveelheid"],
            "UOM": ["eenheid"],
            "vat_number": "btw_nummer",
            "date": ["datum"],
            "full_file_path": F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace("full_file_path", "dbfs:", ""), "%20", " "), "%3E", ">"), " ", "_")
        },
        # "csv_read_config": { # NEW: Configuration for reading the staged CSVs with pandas
        #     "header": 3, # 0-indexed.
        #     "sep": ",",
        #     "encoding": "latin1", # Common for European characters
        #     "skip_blank_lines": True,
        #     "on_bad_lines": "skip", # 'warn', 'skip', or a callable
        # }
}

# COMMAND ----------

# static wholesaler config:
wholesaler_name = "Spuntini_Degroof_5526040_V0"
base_file_path = "/Volumes/d_dnst_000_landing/volumes/sellout_data/5526040___Spuntini_Degroof___V0/"
# base_file_path = "/Volumes/d_dnst_000_landing/volumes/sellout_data/5526040___Spuntini_Degroof___V0/2023"

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

# (
#     get_fn_flowbase().alias('df')
#     # .filter(F.col("full_file_path").contains("2023"))
#     .display()
# )

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
