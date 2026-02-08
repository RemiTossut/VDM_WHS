# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from bronze_functions import *
from bronze_variables import *

# COMMAND ----------

# column mapping config:
config = {
        "folder_name": "5526091 - Spuntini Deerlijk - V1", 
        "column_mapping": {
            "wholesaler_name": F.lit("Spuntini_Deerlijk_5526091"),
            "wholesaler_id": F.lit("5526091"),
            "customer_address": ["adres"], 
            "customer_city": ["stad_gemeente"], 
            "customer_postal_code": ["postcode"],
            "customer_country": F.lit("BE"),
            "customer_name": ["klantnaam"],
            "owner_name": None,
            "customer_number": ["klantnr"],
            "product_number": ["vendoritemno", "Artikelnummer_leverancier"],
            "article_name": ["omschrijving"],
            "EAN": None,
            "total_amount": ["aantal"],
            "UOM": ["eenheid"],
            "vat_number": None,
            "date": "verzenddatum",
            "full_file_path": F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace("full_file_path", "dbfs:", ""), "%20", " "), "%3E", ">"), " ", "_")
        }
}

# COMMAND ----------

# static wholesaler config:
wholesaler_name = "Spuntini_Deerlijk_5526091_V1"
base_file_path = "/Volumes/d_dnst_000_landing/volumes/sellout_data/5526091___Spuntini_Deerlijk___V1/"

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
