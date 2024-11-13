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
table_name = "DimProduct"
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

# Creating the DimProduct table in gold layer
spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {gold_target_table_name} (
        ProductKey BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
        ProductAlternateKey STRING,
        ProductSubcategoryKey INT,
        WeightUnitMeasureCode STRING,
        SizeUnitMeasureCode STRING,
        EnglishProductName STRING NOT NULL,
        SpanishProductName STRING, --NOT NULL
        FrenchProductName STRING, --NOT NULL
        StandardCost DECIMAL(19, 4),
        FinishedGoodsFlag BOOLEAN NOT NULL,
        Color STRING, --NOT NULL
        SafetyStockLevel SMALLINT,
        ReorderPoint SMALLINT,
        ListPrice DECIMAL(19, 4),
        Size STRING,
        SizeRange STRING,
        Weight FLOAT,
        DaysToManufacture INT,
        ProductLine STRING,
        DealerPrice DECIMAL(19, 4),
        Class STRING,
        Style STRING,
        ModelName STRING,
        LargePhoto BINARY,
        EnglishDescription STRING,
        FrenchDescription STRING,
        ChineseDescription STRING,
        ArabicDescription STRING,
        HebrewDescription STRING,
        ThaiDescription STRING,
        GermanDescription STRING,
        JapaneseDescription STRING,
        TurkishDescription STRING,
        StartDate TIMESTAMP,
        EndDate TIMESTAMP,
        Status STRING
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
df_Product = dict_df_source_silver_tables['df_Product']
df_ProductModelProductDescriptionCulture = dict_df_source_silver_tables['df_ProductModelProductDescriptionCulture']
df_ProductDescription = dict_df_source_silver_tables['df_ProductDescription']
df_ProductModel = dict_df_source_silver_tables['df_ProductModel']
df_ProductCostHistory = dict_df_source_silver_tables['df_ProductCostHistory']
df_ProductListPriceHistory = dict_df_source_silver_tables['df_ProductListPriceHistory']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Combining all tables:

# COMMAND ----------

df_ppmpdc_ppd = (
    df_ProductModelProductDescriptionCulture.alias('ppmpdc')
        .join(
            df_ProductDescription.alias('ppd'), 
            on=F.col('ppmpdc.ProductDescriptionID') == F.col('ppd.ProductDescriptionID'), 
            how='left'
        )
        .groupBy("ProductModelID").pivot("CultureID").agg(F.first("Description"))
)
df_ppmpdc_ppd = df_ppmpdc_ppd.toDF(*[c.strip() for c in df_ppmpdc_ppd.columns])

# Joining ProductCostHistory and df_ProductListPriceHistory
df_ppch_pplph = df_ProductCostHistory.alias('ppch').join(
    other=df_ProductListPriceHistory.alias('ppch_pplph'), 
    on=(
        (F.col('ppch.ProductID') == F.col('ppch_pplph.ProductID')) &
        (F.col('ppch.StartDate') == F.col('ppch_pplph.StartDate')) & (
         (F.col('ppch.EndDate') == F.col('ppch_pplph.EndDate')) |
          (F.col('ppch.EndDate').isNull() & F.col('ppch_pplph.EndDate').isNull()) 
        )
       
    ),
    how='left'
).select(
    F.col('ppch.ProductID').alias('ProductID'),
    F.col('ppch.StartDate').alias('StartDate'),
    F.col('ppch.EndDate').alias('EndDate'),
    F.col('ppch_pplph.ListPrice').alias('ListPrice'),
    F.col('ppch.StandardCost').alias('StandardCost')
)

# Combining all source tables through joins to create the DimProduct table
df_DimProduct = (
    df_Product.alias('pp').join(
        other=df_ProductModel.alias('ppm'), 
        on=F.col('pp.ProductModelID') == F.col('ppm.ProductModelID'), 
        how='left'
    ).join(
        other=df_ppch_pplph.alias('ppch_pplph'), 
        on=F.col('pp.ProductID') == F.col('ppch_pplph.ProductID'), 
        how='left'
    ).join(
        other=df_ppmpdc_ppd.alias('ppmpdc_ppd'), 
        on=F.col('pp.ProductModelID') == F.col('ppmpdc_ppd.ProductModelID'), 
        how='left'
    ).select(
        F.col('pp.ProductNumber').alias('ProductAlternateKey'),
        F.col('pp.ProductSubcategoryID').alias('ProductSubcategoryKey'),
        F.col('pp.WeightUnitMeasureCode').alias('WeightUnitMeasureCode'),
        F.col('pp.SizeUnitMeasureCode').alias('SizeUnitMeasureCode'),
        F.col('pp.Name').alias('EnglishProductName'),
        F.lit(None).alias('SpanishProductName'),
        F.lit(None).alias('FrenchProductName'),
        F.col('ppch_pplph.StandardCost').alias('StandardCost'),
        F.col('pp.FinishedGoodsFlag').alias('FinishedGoodsFlag'),
        F.col('pp.Color').alias('Color'),
        F.col('pp.SafetyStockLevel').alias('SafetyStockLevel'),
        F.col('pp.ReorderPoint').alias('ReorderPoint'),
        F.col('ppch_pplph.ListPrice').alias('ListPrice'),
        F.col('pp.Size').alias('Size'),
        F.lit(None).alias('SizeRange'),
        F.col('pp.Weight').alias('Weight'),
        F.col('pp.DaysToManufacture').alias('DaysToManufacture'),
        F.col('pp.ProductLine').alias('ProductLine'),
        F.lit(None).alias('DealerPrice'),
        F.col('pp.Class').alias('Class'),
        F.col('pp.Style').alias('Style'),
        F.col('ppm.Name').alias('ModelName'),
        F.lit(None).alias('LargePhoto'),
        F.col('ppmpdc_ppd.en').alias('EnglishDescription'),
        F.col('ppmpdc_ppd.fr').alias('FrenchDescription'),
        F.col('ppmpdc_ppd.zh-cht').alias('ChineseDescription'),
        F.col('ppmpdc_ppd.ar').alias('ArabicDescription'),
        F.col('ppmpdc_ppd.he').alias('HebrewDescription'),
        F.col('ppmpdc_ppd.th').alias('ThaiDescription'),
        F.lit(None).alias('GermanDescription'),
        F.lit(None).alias('JapaneseDescription'),
        F.lit(None).alias('TurkishDescription'),
        F.col('ppch_pplph.StartDate').alias('StartDate'),
        F.col('ppch_pplph.EndDate').alias('EndDate'),
        F.when(F.col('ppch_pplph.EndDate').isNull(), 'Current').otherwise(None).alias('Status')
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upserting data to gold layer:

# COMMAND ----------

# Upserting data to gold layer
upsert_delta_table(
  df_source_table=df_DimProduct,
  sink_table_name=gold_target_table_name,
  primary_keys=primary_keys,
  auto_generated_column=['ProductKey']
)
