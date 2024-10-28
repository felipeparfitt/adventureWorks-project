# Databricks notebook source
# MAGIC %md
# MAGIC # ETL: Silver to Gold Layer
# MAGIC
# MAGIC This notebook provides operations to handle reading, combining, and upserting data between the silver and gold layers in batch mode.
# MAGIC
# MAGIC ## Batch Mode
# MAGIC - **Reading Data from Silver:** Reads all necessary tables from the silver layer of a specified environment and returns the DataFrames for further processing, ensuring that the data is successfully retrieved.
# MAGIC
# MAGIC - **Combining Data:** Merging datasets from the silver layer and selecting relevant columns from each.
# MAGIC
# MAGIC - **Upserting Data to Silver:** Upserts data from a source DataFrame into a Delta table in the gold layer, updating existing records and inserting new ones based on primary keys.

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label='Enter the environment in lower case')
env = dbutils.widgets.get('env')

# COMMAND ----------

# MAGIC %run "../utils/common_variables"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Info (Source/Target)

# COMMAND ----------

# Table info
table_name = "DimPromotion"
source_tables = dw_adventureworks_tables_info[table_name]['source_tables']
primary_keys = dw_adventureworks_tables_info[table_name]['primary_keys']

# Source info
silver_source_table_names = [
    f"{catalog_name}.silver.{adventureworks_tables_info[source_table]['schema_name']}_{source_table}" for source_table in source_tables
]

# Target info
gold_target_path = f"{gold_path}/{table_name}"
gold_target_table_name = f"{catalog_name}.gold.{table_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the gold table

# COMMAND ----------

# Creating the DimPromotion table in gold layer
spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {gold_target_table_name} (
        PromotionKey BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY,
        PromotionAlternateKey INT,
        EnglishPromotionName STRING,
        SpanishPromotionName STRING,
        FrenchPromotionName STRING,
        DiscountPct FLOAT,
        EnglishPromotionType STRING,
        SpanishPromotionType STRING,
        FrenchPromotionType STRING,
        EnglishPromotionCategory STRING,
        SpanishPromotionCategory STRING,
        FrenchPromotionCategory STRING,
        StartDate TIMESTAMP NOT NULL,
        EndDate TIMESTAMP,
        MinQty INT,
        MaxQty INT
    )
    USING DELTA
    LOCATION '{gold_target_path}'
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')
""")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Reading/Combining/Upserting data from silver to gold layer

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading all tables:

# COMMAND ----------

# Reading all source tables from silver layer and storing them in a dictionary
dict_df_source_silver_tables = reading_all_silver_tables(silver_source_table_names)
# Organizing the variables names
df_SpecialOffer = dict_df_source_silver_tables['df_SpecialOffer']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Combining all tables:

# COMMAND ----------

 # Selecting and changing all needed columns
 df_DimPromotion = df_SpecialOffer.select(
    F.col('SpecialOfferID').alias('PromotionAlternateKey'),
    F.col('Description').alias('EnglishPromotionName'),
    F.lit(None).alias('SpanishPromotionName'),
    F.lit(None).alias('FrenchPromotionName'),
    F.col('DiscountPct').alias('DiscountPct'),
    F.col('Type').alias('EnglishPromotionType'),
    F.lit(None).alias('SpanishPromotionType'),
    F.lit(None).alias('FrenchPromotionType'),
    F.col('Category').alias('EnglishPromotionCategory'),
    F.lit(None).alias('SpanishPromotionCategory'),
    F.lit(None).alias('FrenchPromotionCategory'),
    F.col('StartDate').alias('StartDate'),
    F.col('EndDate').alias('EndDate'),
    F.col('MinQty').alias('MinQty'),
    F.col('MaxQty').alias('MaxQty')
 )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upserting data to gold layer:

# COMMAND ----------

# Upserting data to gold layer
upsert_delta_table(
  df_source_table=df_DimPromotion,
  sink_table_name=gold_target_table_name,
  primary_keys=primary_keys,
  auto_generated_column=['PromotionKey']
)
