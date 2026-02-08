# Databricks notebook source
# %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from bronze_functions import *
from bronze_variables import *

# COMMAND ----------

# column mapping config:
config = {
        "folder_name": "5526147 - Ilis - V0",
        "column_mapping": {
            "wholesaler_name": F.lit("Ilis_5526147"),
            "wholesaler_id": F.lit("5526147"),
            "customer_address": ["rue", "adresse_1"],
            "customer_city": ["ville","localité"],
            "customer_postal_code": "cp",
            "customer_country": "pays",
            "customer_name": F.regexp_extract(F.col("client"), "(.*)/.*", 1),
            "owner_name": None,
            "customer_number": F.regexp_extract(F.col("client"), ".*/(.*)", 1),
            "product_number": ["numéro_article_ilis","article"],
            "article_name": "description_article",
            "EAN": None,
            "total_amount": ["qv_ds_aa", "qté"],
            "UOM": "uom",
            "vat_number": ["n_tva", "tva"],
            "date": ["jour","date"],
            "full_file_path": F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace("full_file_path", "dbfs:", ""), "%20", " "), "%3E", ">"), " ", "_")
        }
}

# COMMAND ----------

# static wholesaler config:
wholesaler_name = "Ilis_5526147_V0"
base_file_path = "/Volumes/d_dnst_000_landing/volumes/sellout_data/5526147___Ilis___V0/"

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
