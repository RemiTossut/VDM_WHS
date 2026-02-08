# Databricks notebook source
# MAGIC %run ../../../modules/load_modules
# MAGIC

# COMMAND ----------

from imports import *

# COMMAND ----------

source_catalog = "d_dnst_020_silver"
source_schema = "silver_01"
source_table = "01_sellout_consolidated"

target_catalog = "d_dnst_020_silver"
target_schema = "silver_02"
target_table = "02_sellout_consolidated"


# COMMAND ----------

# %sql
# DROP TABLE d_dnst_020_silver.silver_02.`02_sellout_consolidated`

# COMMAND ----------

df_source = spark.table(f"{source_catalog}.{source_schema}.{source_table}")

# COMMAND ----------

DATE_PARSE_STEPS = [
    # 1. NEW: High priority for standard YYYY-MM-DD (Fixes '2023-01-18')
    {"type": "yyyy_mm_dd_hyphen"}, 
    
    # 2. NEW: Handle DD/MM/YYYY and DD.MM.YYYY (Fixes '01/05/2023')
    {"type": "dd_mm_yyyy_slash"}, 

    # 3. Existing MM_YY underscore formats
    {"type": "mm_yy_underscore"},
    {"type": "mm_yyyy_underscore"},  # '12_2023'
    
    # 4. Existing M/YYYY and M-YYYY formats (parses to last day of month)
    # NOTE: The regex here is modified to support both slash and hyphen separators
    {"type": "mm_yyyy_lastday", "regex": r"^\d{1,2}[/-]\d{4}$"}, 
    
    # 5. Existing DD_MM_YYYY underscore formats
    {"type": "dd_mm_yyyy_underscore"},                        # '05_01_2023'
    
    # 6. Existing Quarter formats
    {"type": "quarter_last_day",     "pattern": r"^(\d{4})_Q([1-4])$"},    # '2023_Q1'
    # {"type": "quarter_last_day_rev", "pattern": r"^Q([1-4])_(\d{4})$"},    # 'Q1_2023'
    {"type": "quarter_last_day_rev", "pattern": r"Q([1-4])_(\d{4})"},    # 'Q1_2023'
    
    # 7. Existing Date formats
    {"type": "date_fmt", "fmt": "dd-MMM-yy"},                # '05-Jan-23'
    {"type": "date_fmt", "fmt": "d-MMM-yy"},                 # '5-Jan-23'
    {"type": "date_fmt", "fmt": "dd-MMM-yyyy"},              # '05-Jan-2023'
    {"type": "date_fmt", "fmt": "d-MMM-yyyy"},               # '5-Jan-2023'
    
    # 8. Existing YYYYMMDD
    {"type": "yyyyMMdd"},                                    # '20230105'
    
    # 9. Existing Timestamp formats
    {"type": "ts_fmt", "fmt": "yyyy-MM-dd HH:mm:ss"},        # '2025-02-26 00:00:00'
    {"type": "ts_fmt", "fmt": "yyyy-MM-dd HH:mm:ss.SSS"},    # '2025-02-26 00:00:00.000'
    
    # 10. Last-resort tolerant parses
    {"type": "ts_try"},                                      # ISO-like timestamps
    {"type": "date_try"}                                     # last-resort tolerant date parse
]

# COMMAND ----------

def _quarter_end_month(q_col: F.Column) -> F.Column:
    """Map quarter number ('1'..'4') to month string ('03','06','09','12')."""
    return (
        F.when(q_col == F.lit('1'), F.lit('03'))
         .when(q_col == F.lit('2'), F.lit('06'))
         .when(q_col == F.lit('3'), F.lit('09'))
         .when(q_col == F.lit('4'), F.lit('12'))
    )

def _build_date_expr(col_name: str) -> F.Column:
    """
    Builds a coalesced Column expression from DATE_PARSE_STEPS.
    Uses only native PySpark functions (serverless-safe).
    Returns a DateType column (NULL when unparsable).
    """
    c = F.col(col_name)
    exprs = []

    for step in DATE_PARSE_STEPS:
        t = step["type"]
        
        # --- Simplified Logic Blocks ---
        
        # 1. 'YYYY-MM-DD'
        if t == "yyyy_mm_dd_hyphen":
            # Using F.to_date for this standard ISO format handles the check implicitly
            # but we keep the regex for strictness/speed, matching original logic.
            rgx = r'^\d{4}-\d{1,2}-\d{1,2}$'
            exprs.append(F.when(c.rlike(rgx), F.to_date(c, 'yyyy-MM-dd')))

        elif t == "quarter_last_day_rev":
            pat = step["pattern"] # 'Q#_YYYY'
            q = F.regexp_extract(c, pat, 1)
            yr = F.regexp_extract(c, pat, 2)
            end_month = _quarter_end_month(q)
            date_str = F.concat_ws('-', yr, end_month, F.lit('01'))
            exprs.append(
                F.when(c.rlike(pat), F.last_day(F.to_date(date_str, 'yyyy-MM-dd')))
            )
        
        # 2. 'DD/MM/YYYY' or 'DD.MM.YYYY'
        elif t == "dd_mm_yyyy_slash": 
            rgx = r'^\d{1,2}[./]\d{1,2}[./]\d{4}$'
            # Note: PySpark's F.to_date will tolerate dot/hyphen separators 
            # when given the slash pattern, which matches your original logic.
            exprs.append(F.when(c.rlike(rgx), F.to_date(c, 'dd/MM/yyyy')))

        # 3. 'MM_YYYY' or 'MM_YY' → last day of month
        elif t == "mm_yyyy_underscore":
            rgx = r'^(\d{2})_(\d{4})$'
            date_str = F.concat_ws('-', F.regexp_extract(c, rgx, 2), F.regexp_extract(c, rgx, 1), F.lit('01'))
            exprs.append(
                F.when(c.rlike(rgx), F.last_day(F.to_date(date_str, 'yyyy-MM-dd')))
            )

        elif t == "mm_yy_underscore":
            rgx = r'^(\d{2})_(\d{2})$'
            # Concats to 'YY-MM-01' and parses using 'yy-MM-dd' format
            date_str = F.concat_ws('-', F.regexp_extract(c, rgx, 2), F.regexp_extract(c, rgx, 1), F.lit('01'))
            exprs.append(
                F.when(c.rlike(rgx), F.last_day(F.to_date(date_str, 'yy-MM-dd')))
            )
            
        # 4. 'M/YYYY' or 'MM/YYYY' (slash/hyphen separated) -> last day of month
        elif t == "mm_yyyy_lastday":
            rgx = step["regex"] # r"^\d{1,2}[/-]\d{4}$"
            
            # Extract month (1 or 2 digits) and pad it
            month_part = F.regexp_extract(c, r"^(\d{1,2})[/\-]\d{4}$", 1)
            padded_month = F.lpad(month_part, 2, '0')

            # Extract year (YYYY)
            year = F.regexp_extract(c, r"^\d{1,2}[/\-](\d{4})$", 1) 

            # Concatenate to a standard YYYY-MM-01 format string
            date_string = F.concat_ws('-', year, padded_month, F.lit('01'))

            exprs.append(
                F.when(c.rlike(rgx), F.last_day(F.to_date(date_string, 'yyyy-MM-dd')))
            )

        # 5. 'DD_MM_YYYY'
        elif t == "dd_mm_yyyy_underscore":
            rgx = r'^(\d{2})_(\d{2})_(\d{4})$'
            # Concatenate to YYYY-MM-DD
            date_str = F.concat_ws('-', F.regexp_extract(c, rgx, 3), F.regexp_extract(c, rgx, 2), F.regexp_extract(c, rgx, 1))
            exprs.append(
                F.when(c.rlike(rgx), F.to_date(date_str, 'yyyy-MM-dd'))
            )

        # 6. Quarter formats
        elif t == "quarter_last_day":
            pat = step["pattern"] # 'YYYY_Q#'
            yr = F.regexp_extract(c, pat, 1)
            q = F.regexp_extract(c, pat, 2)
            end_month = _quarter_end_month(q)
            date_str = F.concat_ws('-', yr, end_month, F.lit('01'))
            exprs.append(
                F.when(c.rlike(pat), F.last_day(F.to_date(date_str, 'yyyy-MM-dd')))
            )



        # 7. Date formats (dd-MMM-yy, etc.)
        elif t == "date_fmt":
            exprs.append(F.to_date(c, step["fmt"]))

        # 8. YYYYMMDD
        elif t == "yyyyMMdd":
            # Using F.when(F.length(c) == 8, ...) is slightly cleaner, but rlike(r'^\d{8}$') 
            # matches your strict original intent.
            exprs.append(F.when(c.rlike(r'^\d{8}$'), F.to_date(c, 'yyyyMMdd')))

        # 9. Timestamp formats
        elif t == "ts_fmt":
            # Chaining F.to_date(F.to_timestamp(...)) is explicit.
            exprs.append(F.to_date(F.to_timestamp(c, step["fmt"])))

        # 10. Last-resort tolerant parses
        elif t == "ts_try":
            exprs.append(F.to_date(F.to_timestamp(c)))

        elif t == "date_try":
            exprs.append(F.to_date(c))

    # Coalesce to the first non-null parse result.
    exprs = [e for e in exprs if e is not None]

    if not exprs:
        return F.lit(None).cast(DateType())

    return F.coalesce(*exprs).cast(DateType())

# COMMAND ----------

def process_date_column(df):
    if "date" not in df.columns:
        return df

    tmp_col_name = "date_str_temp"
    path_col_name = "date_path_temp" 

    df_res = df.withColumn(path_col_name, F.trim(F.col("date").cast("string")))

    # FIXED PATTERN: Use (?:...) for inner OR logic so Group 1 is ALWAYS the full match
    FILE_PATH_DATE_PATTERN = r"((?:\d{2}_\d{2}_\d{4})|(?:\d{2}_\d{4})|(?:\d{2}_\d{2})|(?:[qQ][1-4]_\d{4})|(?:\d{4}_[qQ][1-4]))"
    
    is_full_path = F.col(path_col_name).rlike(r"(volumes|datalake)/") 
    
    # Check if the string is a clean date format (Added Case Insensitivity (?i))
    is_clean_date_string = F.col(path_col_name).rlike(
        r"(?i)(^\d{1,2}[/-]\d{2,4}$|^\d{1,2}_\d{2,4}$|^\d{2}_\d{2}_\d{4}$|^[qQ][1-4]_\d{4}$|^\d{4}_[qQ][1-4]$|^\d{4}-\d{1,2}-\d{1,2}.*|^\d{8}$)"
    ) 

    df_res = df_res.withColumn(
        tmp_col_name, 
        F.when(is_clean_date_string, F.col(path_col_name))
         .when(is_full_path, F.regexp_extract(F.col(path_col_name), FILE_PATH_DATE_PATTERN, 1))
         .otherwise(F.lit(None).cast("string"))
    )

    date_expr = _build_date_expr(tmp_col_name)
    
    df_res = (
        df_res.withColumn("date_parsed", date_expr) # Named it date_parsed to avoid overwriting immediately for safety
        .withColumn("date", F.coalesce(F.col("date_parsed"), F.col("date"))) # Fallback if parsing failed
        .drop("date_path_temp","date_str_temp", "date_parsed")
    )

    return df_res

# COMMAND ----------

df_with_date = process_date_column(df_source)

# COMMAND ----------

country_map = {
    "België": "BE",
    "Belgium": "BE",
    "Belgie": "BE",
    "Belgique": "BE",
    "France": "FR",
    "Frankrijk": "FR",
    "Nederland": "NL",
    "Duitsland": "DE"
}
mapping_expr = F.create_map([F.lit(x) for x in chain(*country_map.items())])

#Normalize country names to ISO codes
df_with_country = df_with_date.withColumn(
    "customer_country",
    F.trim(F.coalesce(mapping_expr[F.col("customer_country")], F.col("customer_country")))
)

# COMMAND ----------

clean_raw = F.regexp_replace(F.col("vat_number"), r"(?i)VAT|BTW|[^A-Z0-9]", "")

# 2. Extract ONLY digits for validation (length and zero checks)
only_digits = F.regexp_replace(clean_raw, r"[^0-9]", "")

# 3. Format the result
# Check if it already starts with a 2-letter country code (like BE)
# If not, prepend the customer_country
final_vat = F.when(
    clean_raw.rlike("^[A-Z]{2}"), clean_raw
).otherwise(
    F.concat(F.coalesce(F.col("customer_country"), F.lit("")), only_digits)
)

# 4. Final Assignment with Validation
df_with_vat = df_with_country.withColumn(
    "vat_number",
    F.when(
        (only_digits.isNull()) | 
        (only_digits == "") | 
        (F.length(only_digits) < 5) |
        (only_digits.rlike("^0+$")), 
        F.lit(None)
    ).otherwise(final_vat)
)

# COMMAND ----------

final_df = (
    df_with_vat
    .withColumn(
    "customer_postal_code",
    F.regexp_replace(F.col("customer_postal_code"), "\.0$", "")
    )
    .withColumn(
    "product_number",
    F.regexp_replace(F.col("product_number"), "\.0$", "")
    )
    .withColumn("customer_number", F.ltrim(F.col("customer_number")))
    .withColumn(
        "customer_number",
        F.regexp_replace(F.col("customer_number"), "\\.0$", "")
    )
    .withColumn("ean", F.regexp_replace(F.format_number(F.col("ean").cast("double"), 0), ",", ""))
   .withColumn("date", 
    F.coalesce(
        F.to_date(F.col("date"), "dd/MM/yyyy"),
        F.to_date(F.col("date"), "yyyy-MM-dd")
    )
    )
    .withColumn("customer_postal_code", F.trim(F.col("customer_postal_code")))
)

# COMMAND ----------

# (
#     final_df
#     .filter(F.col("full_file_path").contains("/Volumes/d_dnst_000_landing/volumes/sellout_data/5525973___Spuntini_Free_Foods___V0/2023/Data_Vandemoortele_FreeFoods_okt_dec_2023_Blad1.csv"))
#     .select("date")
#     .display()
# )

# COMMAND ----------

df_flowbase = final_df
flowbase = {"catalog_name": target_catalog,
            "schema_name": target_schema,
            "table_name":target_table,
            "operation_type": "overwrite",
            "dependencies": [
               f"{source_catalog}.{source_schema}.{source_table}"
               ]
            }
