# Databricks notebook source
# MAGIC %run ../../../modules/load_modules

# COMMAND ----------

from imports import *

# COMMAND ----------

source_sellout_catalog = "d_dnst_020_silver"
source_sellout_schema = "silver_02"
source_sellout_table = "02_sellout_consolidated"

source_uom_catalog = "d_dnst_010_bronze"
source_uom_schema = "sharepoint_list"
source_uom_table = "wholesaler_uom_conversion_table"

source_c4s_account_catalog = "d_dnst_010_bronze"
source_c4s_account_schema = "sap_bw"
source_c4s_account_table = "c4c_account"

source_t006_catalog = "d_dnst_010_bronze"
source_t006_schema = "sharepoint_list"
source_t006_table = "uom_assign_internal_to_language_dependent_unit_t006a"

source_customer_material_info_record_catalog = "d_dnst_010_bronze"
source_customer_material_info_record_schema = "sap_bw"
source_customer_material_info_record_table = "customer_material_info_record"  # = KNMT

source_material_unit_of_measure_conversion_catalog = "d_dnst_010_bronze"
source_material_unit_of_measure_conversion_schema = "sap_bw"
source_material_unit_of_measure_conversion_table = "material_unit_of_measure_conversion" # = MARM

target_catalog = "d_dnst_020_silver"
target_schema = "silver_03"
target_table = "03_sellout_sales"


# COMMAND ----------

# %sql 
# DROP TABLE d_dnst_020_silver.silver_03.03_sellout_sales

# COMMAND ----------

df_source_sellout = spark.table(f"{source_sellout_catalog}.{source_sellout_schema}.{source_sellout_table}")
df_source_uom = spark.table(f"{source_uom_catalog}.{source_uom_schema}.{source_uom_table}")

df_source_c4s_account = spark.table(f"{source_c4s_account_catalog}.{source_c4s_account_schema}.{source_c4s_account_table}")

df_customer_material_info_record = spark.table(f"{source_customer_material_info_record_catalog}.{source_customer_material_info_record_schema}.{source_customer_material_info_record_table}")
df_material_unit_of_measure_conversion = spark.table(f"{source_material_unit_of_measure_conversion_catalog}.{source_material_unit_of_measure_conversion_schema}.{source_material_unit_of_measure_conversion_table}")

df_t006 = spark.table(f"{source_t006_catalog}.{source_t006_schema}.{source_t006_table}")

# COMMAND ----------

df_sellout_with_external_id = (
    df_source_sellout.alias("n")
    .join(
        df_source_c4s_account.alias("c4s"),
        on = F.expr("""
                    ltrim('0',n.wholesaler_id) =  ltrim('0',c4s.c4cu_account_id)
        """),
        how = "left"
    )
    .selectExpr(
        "n.*",
        "c4s.c4cu_customer_id"
    )
)

# COMMAND ----------

df_relevant_columns = (
    df_sellout_with_external_id
    .select(
        "customer_number",
        "product_number",
        "article_name",
        "EAN",
        "total_amount",
        "wholesaler_name",
        "date",
        "full_file_path",
        "UoM",
        "wholesaler_id",
        "c4cu_customer_id"
    )
    .withColumn(
    "dimdateid",
    F.date_format(F.col("date"), "yyyyMMdd").cast("int")
    )
    .withColumn(
    "sharepoint_file_name",
    F.regexp_replace(F.element_at(F.split(F.col("full_file_path"), "/"), -1), "\.csv", "")
)
)

# COMMAND ----------

df_joined = (
    df_relevant_columns
    .withColumn("unique_id", F.monotonically_increasing_id())
    .alias("n")
    .join(
        df_customer_material_info_record.alias("d"),
        on = F.expr("""
            ltrim('0', n.product_number) = ltrim('0', d.cumi_customermaterialnumber_id)
            AND
            ltrim('0',n.c4cu_customer_id) =  ltrim('0',d.cumi_customernumber_id)
        """),
        how = "left"
    )
    .selectExpr(
        "n.*",
        "d.cumi_material_id",
        "d.cumi_customermaterialnumber_id"
    )
)

# COMMAND ----------

window_spec = Window.partitionBy("unique_id").orderBy(F.lit(1))

df_deduplicated = (
    df_joined.withColumn("row_number", F.row_number().over(window_spec))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

# COMMAND ----------

df_source_uom_converted = (
    df_source_uom.alias("uom")
    .join(
        df_t006.alias("t"),
        on = F.expr("""
            uom.converted_uom = t.converted_uom
            and
            t.language = 'EN'
        """),
        how = "left"
    )
    .selectExpr(
        "uom.uom",
        "uom.wholesaler",
        "uom.wholesaler_name",
        "uom.converted_uom",
        "t.muom_unit_id"
    )
)

# COMMAND ----------

df_with_oum_conversion = (
    df_deduplicated.alias("d")
    .join(
        df_source_uom_converted.alias("t"),
        on = F.expr("d.wholesaler_name == t.wholesaler_name AND d.UoM == t.uom"),
        how = "left"
    )
    # .drop("d.uom")
    .selectExpr(
        "d.*",
        "t.converted_uom",
        "t.muom_unit_id",
        # "uom.uom"
    )
)

# COMMAND ----------

df_deduplicated_with_uom = (
    df_with_oum_conversion.withColumn("row_number", F.row_number().over(window_spec))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

# COMMAND ----------

df_with_kg_conversion = (
    df_deduplicated_with_uom.alias("d")
    .join(
        df_material_unit_of_measure_conversion.alias("m_1"),
        on = F.expr("""
                    ltrim(0,d.cumi_customermaterialnumber_id) == ltrim(0,m_1.muom_material_id)
                    --ltrim(0,d.cumi_material_id) == ltrim(0,m_1.muom_material_id)
                    and 
                    --d.converted_uom = m_1.muom_unit_id
                    --change using mapping table:
                    d.muom_unit_id = m_1.muom_unit_id
            """),
        how = "left"
    )
    .join(
        df_material_unit_of_measure_conversion.alias("m_2"),
        on=F.expr("""
                  ltrim(0,d.EAN) == ltrim(0,m_2.muom_eanupc_id)
            """), 
        how = "left"
    )
    .select(
        F.col("d.*"),
        F.coalesce(F.col("m_2.muom_denominator_qty"), F.col("m_1.muom_denominator_qty"))
         .alias("muom_denominator_qty"),
        F.coalesce(F.col("m_2.muom_numerator_qty"), F.col("m_1.muom_numerator_qty"))
         .alias("muom_numerator_qty"),
        F.col("m_2.muom_material_id").alias("muom_material_id")
    )
    .withColumn("cumi_customermaterialnumber_id", 
                F.coalesce(
                    F.regexp_replace(F.col("muom_material_id"), '^0+', ''),
                    F.col("cumi_customermaterialnumber_id"), 
                )
    )
    .drop("muom_material_id")
)

# COMMAND ----------

df_deduplicated_with_kg = (
    df_with_kg_conversion.withColumn("row_number", F.row_number().over(window_spec))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

# COMMAND ----------

df_with_kilo_amount = (
    df_deduplicated_with_kg
    .withColumn("KgL", F.col("total_amount") * F.col("muom_numerator_qty")/F.col("muom_denominator_qty"))
)

# COMMAND ----------

enable_columnmapping_sql_code = f"""
ALTER TABLE {target_catalog}.{target_schema}.{target_table} 
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
"""

# COMMAND ----------

df_flowbase = df_with_kilo_amount
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name":target_table,
            "operation_type": "overwrite",
            "deploy_strategy": "column",
            "dependencies": [
              f"{source_sellout_catalog}.{source_sellout_schema}.{source_sellout_table}",
              f"{source_customer_material_info_record_catalog}.{source_customer_material_info_record_schema}.{source_customer_material_info_record_table}",
              f"{source_material_unit_of_measure_conversion_catalog}.{source_material_unit_of_measure_conversion_schema}.{source_material_unit_of_measure_conversion_table}",

              f"{source_uom_catalog}.{source_uom_schema}.{source_uom_table}"
               ],
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
