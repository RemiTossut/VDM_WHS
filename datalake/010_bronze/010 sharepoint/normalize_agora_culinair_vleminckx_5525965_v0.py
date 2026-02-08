# Databricks notebook source
# %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from bronze_functions import *
from bronze_variables import *

# COMMAND ----------

# column mapping config:
config = {
        "folder_name": "5525965 - Agora Culinair Vleminckx - V0",
        "column_mapping": {
            "wholesaler_name": F.lit("Agora_Culinair_Vleminckx_5525965"),
            "wholesaler_id": F.lit("5525965"),
            "customer_address": "adres",
            "customer_city": "gemeente",
            "customer_postal_code": "pc",
            "customer_country": None,
            "customer_name": "naam_zaak",
            "owner_name": None,
            "customer_number": "klant",
            "product_number": "art_kode_lev",
            "article_name": "omschr_nl",
            # "EAN": "barcode",
            "EAN": F.col("barcode").cast('decimal(18, 0)').cast('string'),
            "total_amount": "aantal_geleverd",
            "UOM": None,
            "vat_number": "btw_nummer",
            "date": "datum",
            "full_file_path": F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace("full_file_path", "dbfs:", ""), "%20", " "), "%3E", ">"), " ", "_")
        }
}

# COMMAND ----------

# static wholesaler config:
wholesaler_name = "Agora_Culinair_Vleminckx_5525965_V0"
base_file_path = "/Volumes/d_dnst_000_landing/volumes/sellout_data/5525965___Agora_Culinair_Vleminckx___V0/"

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
