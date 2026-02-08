# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *
from bronze_functions import *
from bronze_variables import *

# COMMAND ----------

# column mapping config:
config = {
        "folder_name": "5525843 - Solucious - V0",
        "column_mapping": {
            "wholesaler_name": F.lit("Solucious_5525843"),
            "wholesaler_id": F.lit("5525843"),
            "customer_address": "adresse_1_livraison",
            "customer_city": "bur_distrib_livr",
            "customer_postal_code": "c/postal_livraison",
            "customer_country": F.lit("BE"),
            "customer_name": "nom_de_livraison",
            "owner_name": None,
            "customer_number": "c/client_facturé",
            "product_number": "c/article",
            "article_name": "désignation",
            "EAN": "code_barre",
            "total_amount": "qté_livrée_u_v",
            "UOM": "unité_de_vente",
            # "date": "date_de_livraison__",
            "date": "date_de_facture",
            "full_file_path": F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace("full_file_path", "dbfs:", ""), "%20", " "), "%3E", ">"), " ", "_")
        }
}

# COMMAND ----------

# static wholesaler config:
wholesaler_name = "Solucious_5525843_V0"
base_file_path = "/Volumes/d_dnst_000_landing/volumes/sellout_data/5525843___Solucious___V0/"

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
