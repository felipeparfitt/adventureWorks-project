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
table_name = "FactInternetSales"
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

spark.sql(f"drop table if exists {gold_target_table_name}")

# COMMAND ----------

# Creating the FactInternetSales table in gold layer
spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {gold_target_table_name} (
        ProductKey INT,
        OrderDateKey INT,
        DueDateKey INT,
        ShipDateKey INT,
        CustomerKey INT,
        PromotionKey INT,
        CurrencyKey INT,
        SalesTerritoryKey INT,
        SalesOrderNumber STRING,
        SalesOrderLineNumber TINYINT,
        RevisionNumber TINYINT,
        OrderQuantity SMALLINT,
        UnitPrice DECIMAL(19,4),
        ExtendedAmount DECIMAL(19,4),
        UnitPriceDiscountPct FLOAT,
        DiscountAmount FLOAT,
        ProductStandardCost DECIMAL(19,4),
        TotalProductCost DECIMAL(19,4),
        SalesAmount DECIMAL(19,4),
        TaxAmt DECIMAL(19,4),
        Freight DECIMAL(19,4),
        CarrierTrackingNumber STRING,
        CustomerPONumber STRING,
        OrderDate TIMESTAMP,
        DueDate TIMESTAMP,
        ShipDate TIMESTAMP
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
df_SalesOrderDetail = dict_df_source_silver_tables['df_SalesOrderDetail']
df_SalesOrderHeader = dict_df_source_silver_tables['df_SalesOrderHeader'].filter(F.col('OnlineOrderFlag') == F.lit(True))  # In order to reproduce AdvetureWorks DW (only online orders)
df_SalesTerritory = dict_df_source_silver_tables['df_SalesTerritory']
df_CountryRegionCurrency = dict_df_source_silver_tables['df_CountryRegionCurrency'].dropDuplicates(['CountryRegionCode'])

# Reading all gold tables needed
df_DimProduct = spark.read.table(f"{catalog_name}.gold.DimProduct")
df_DimPromotion = spark.read.table(f"{catalog_name}.gold.DimPromotion")
df_DimSalesTerritory = spark.read.table(f"{catalog_name}.gold.DimSalesTerritory")
df_DimCurrency = spark.read.table(f"{catalog_name}.gold.DimCurrency")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Combining all tables:

# COMMAND ----------

# Defining a window function to order the SalesOrderNumber column
window_spec = Window.partitionBy(F.col('ssoh.SalesOrderNumber')).orderBy(F.col('ssod.SalesOrderID'))

# Combining all source tables through joins to create the FactInternetSales table
df_FactInternetSales = (
    df_SalesOrderDetail.alias('ssod').join(
        other=df_SalesOrderHeader.alias('ssoh'),
        on=F.col('ssod.SalesOrderID') == F.col('ssoh.SalesOrderID'),
        how='inner'
    ).join(
        other=df_Product.alias('pp'),
        on=F.col('ssod.ProductID') == F.col('pp.ProductID'),
        how='left'
    ).join(
        other=df_DimProduct.alias('dprod'),
        on=(
            (F.col('pp.ProductNumber') == F.col('dprod.ProductAlternateKey')) &
            (F.col('ssoh.OrderDate').between(F.col('dprod.StartDate'), F.when(F.col('dprod.EndDate').isNull(), F.current_date()).otherwise(F.col('dprod.EndDate'))))
        ),
        how='left'
    ).join(
        other=df_DimPromotion.alias('dprom'),
        on=F.col('ssod.SpecialOfferID') == F.col('dprom.PromotionAlternateKey'),
        how='left'
    ).join(
        other=df_DimSalesTerritory.alias('dst'),
        on=F.col('ssoh.TerritoryID') == F.col('dst.SalesTerritoryAlternateKey'),
        how='left'
    ).join(
        other=df_SalesTerritory.alias('sst'),
        on=F.col('ssoh.TerritoryID') == F.col('sst.TerritoryID'),
        how='left'
    ).join(
        other=df_CountryRegionCurrency.alias('scrc'),
        on=F.col('sst.CountryRegionCode') == F.col('scrc.CountryRegionCode'),
        how='left'
    ).join(
        other=df_DimCurrency.alias('dc'),
        on=F.col('scrc.CurrencyCode') == F.col('dc.CurrencyAlternateKey'),
        how='left'
    ).select(
        F.col('dprod.ProductKey').alias('ProductKey'),
        F.date_format(F.col('ssoh.OrderDate'), 'yyyyMMdd').cast('int').alias('OrderDateKey'),
        F.date_format(F.col('ssoh.DueDate'), 'yyyyMMdd').cast('int').alias('DueDateKey'),
        F.date_format(F.col('ssoh.ShipDate'), 'yyyyMMdd').cast('int').alias('ShipDateKey'),
        F.col('ssoh.CustomerID').alias('CustomerKey'),
        F.col('dprom.PromotionKey').alias('PromotionKey'),
        F.col('dc.CurrencyKey').alias('CurrencyKey'),
        F.col('dst.SalesTerritoryKey').alias('SalesTerritoryKey'),
        F.col('ssoh.SalesOrderNumber').alias('SalesOrderNumber'),
        F.row_number().over(window_spec).alias('SalesOrderLineNumber'),#verifify
        F.col('ssoh.RevisionNumber').alias('RevisionNumber'),
        F.col('ssod.OrderQty').alias('OrderQuantity'),
        F.col('ssod.UnitPrice').alias('UnitPrice'),
        F.col('ssod.LineTotal').alias('ExtendedAmount'),
        F.col('ssod.UnitPriceDiscount').alias('UnitPriceDiscountPct'),
        (F.col('ssod.UnitPriceDiscount')*F.col('ssod.UnitPrice')).alias('DiscountAmount'),
        F.col('dprod.StandardCost').alias('ProductStandardCost'),
        (F.col('ssod.OrderQty') * F.col('dprod.StandardCost')).alias('TotalProductCost'),
        (F.col('ssod.LineTotal') - (F.col('ssod.UnitPriceDiscount')*F.col('ssod.UnitPrice'))).alias('SalesAmount'),
        F.col('ssoh.TaxAmt').alias('TaxAmt'),
        F.col('ssoh.Freight').alias('Freight'),
        F.col('ssod.CarrierTrackingNumber').alias('CarrierTrackingNumber'),
        F.col('ssoh.PurchaseOrderNumber').alias('CustomerPONumber'),
        F.col('ssoh.OrderDate').alias('OrderDate'),
        F.col('ssoh.DueDate').alias('DueDate'),
        F.col('ssoh.ShipDate').alias('ShipDate')
    ).orderBy('SalesOrderNumber', 'SalesOrderLineNumber')
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upserting data to gold layer:

# COMMAND ----------

# Upserting data to gold layer
upsert_delta_table(
  df_source_table=df_FactInternetSales,
  sink_table_name=gold_target_table_name,
  primary_keys=primary_keys
)
