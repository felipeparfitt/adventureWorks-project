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
table_name = "DimSalesTerritory"
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

# Creating the DimSalesTerritory table in gold layer
spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {gold_target_table_name} (
        SalesTerritoryKey BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY,
        SalesTerritoryAlternateKey INT,
        SalesTerritoryRegion STRING NOT NULL,
        SalesTerritoryCountry STRING NOT NULL,
        SalesTerritoryGroup STRING,
        SalesTerritoryImage BINARY
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
df_SalesTerritory = dict_df_source_silver_tables['df_SalesTerritory']
df_CountryRegion = dict_df_source_silver_tables['df_CountryRegion']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Combining all tables:

# COMMAND ----------

# Combining all source tables through joins to create the DimSalesTerritory table
df_DimSalesTerritory = df_SalesTerritory.alias('sst').join(
    df_CountryRegion.alias('pcr'), 
    F.col('sst.CountryRegionCode') == F.col('pcr.CountryRegionCode'), 
    how='left'
 ).select(
    F.col('sst.TerritoryID').alias('SalesTerritoryAlternateKey'),
    F.col('sst.Name').alias('SalesTerritoryRegion'),
    F.col('pcr.Name').alias('SalesTerritoryCountry'),
    F.col('sst.Group').alias('SalesTerritoryGroup'),
    F.lit(None).alias('SalesTerritoryImage')
 )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upserting data to gold layer:

# COMMAND ----------

# Upserting data to gold layer
upsert_delta_table(
  df_source_table=df_DimSalesTerritory,
  sink_table_name=gold_target_table_name,
  primary_keys=primary_keys,
  auto_generated_column=['SalesTerritoryKey']
)

# COMMAND ----------

display(spark.read.table(gold_target_table_name))
