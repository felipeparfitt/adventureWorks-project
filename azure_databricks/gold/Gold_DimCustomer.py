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
table_name = "DimCustomer"
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

# Creating the DimCustomer table in gold layer
spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {gold_target_table_name} (
        CustomerKey INT NOT NULL PRIMARY KEY,
        GeographyKey INT,
        CustomerAlternateKey STRING NOT NULL,
        Title STRING,
        FirstName STRING,
        MiddleName STRING,
        LastName STRING,
        NameStyle BOOLEAN,
        BirthDate DATE,
        MaritalStatus STRING,
        Suffix STRING,
        Gender STRING,
        EmailAddress STRING,
        YearlyIncome STRING,
        TotalChildren TINYINT,
        NumberChildrenAtHome TINYINT,
        EnglishEducation STRING,
        SpanishEducation STRING,
        FrenchEducation STRING,
        EnglishOccupation STRING,
        SpanishOccupation STRING,
        FrenchOccupation STRING,
        HouseOwnerFlag STRING,
        NumberCarsOwned TINYINT,
        AddressLine1 STRING,
        AddressLine2 STRING,
        Phone STRING,
        DateFirstPurchase DATE,
        CommuteDistance STRING
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
df_Customer = dict_df_source_silver_tables['df_Customer']
df_Person = dict_df_source_silver_tables['df_Person']
df_PersonPhone = dict_df_source_silver_tables['df_PersonPhone']
df_EmailAddress = dict_df_source_silver_tables['df_EmailAddress']
df_BusinessEntityAddress = dict_df_source_silver_tables['df_BusinessEntityAddress']
df_Address = dict_df_source_silver_tables['df_Address']
df_vPersonDemographics = dict_df_source_silver_tables['df_vPersonDemographics']


# COMMAND ----------

# MAGIC %md
# MAGIC #### Combining all tables:

# COMMAND ----------

# Combining all source tables through joins to create the DimCustomer table
df_DimCustomer = (
    df_Customer.alias('sc').join(
        other=df_Person.alias('pp'),
        on=F.col('sc.PersonID') == F.col('pp.BusinessEntityID'),
        how='left'
    ).join(
        other=df_PersonPhone.alias('pph'),
        on=F.col('sc.PersonID') == F.col('pph.BusinessEntityID'),
        how='left'
    ).join(
        other=df_EmailAddress.alias('pea'),
        on=F.col('sc.PersonID') == F.col('pea.BusinessEntityID'),
        how='left'
    ).join(
        other=df_BusinessEntityAddress.alias('pbea'),
        on=F.col('sc.PersonID') == F.col('pbea.BusinessEntityID'),
        how='left'
    ).join(
        other=df_Address.alias('pa'),
        on=F.col('pbea.AddressID') == F.col('pa.AddressID'),
        how='left'
    ).join(
        other=df_vPersonDemographics.alias('spd'),
        on=F.col('sc.PersonID') == F.col('spd.BusinessEntityID'),
        how='left'
    ).select(
        F.col('sc.CustomerID').alias('CustomerKey'),
        F.lit(None).alias('GeographyKey'),
        F.col('sc.AccountNumber').alias('CustomerAlternateKey'),
        F.col('pp.Title').alias('Title'),
        F.col('pp.FirstName').alias('FirstName'),
        F.col('pp.MiddleName').alias('MiddleName'),
        F.col('pp.LastName').alias('LastName'),
        F.col('pp.NameStyle').alias('NameStyle'),
        F.col('spd.BirthDate').alias('BirthDate'),
        F.col('spd.MaritalStatus').alias('MaritalStatus'),
        F.col('pp.Suffix').alias('Suffix'),
        F.col('spd.Gender').alias('Gender'),
        F.col('pea.EmailAddress').alias('EmailAddress'),
        F.col('spd.YearlyIncome').alias('YearlyIncome'),
        F.col('spd.TotalChildren').alias('TotalChildren'),
        F.col('spd.NumberChildrenAtHome').alias('NumberChildrenAtHome'),
        F.col('spd.Education').alias('EnglishEducation'),
        F.lit(None).alias('SpanishEducation'),
        F.lit(None).alias('FrenchEducation'),
        F.col('spd.Occupation').alias('EnglishOccupation'),
        F.lit(None).alias('SpanishOccupation'),
        F.lit(None).alias('FrenchOccupation'),
        F.col('spd.HomeOwnerFlag').alias('HouseOwnerFlag'),
        F.col('spd.NumberCarsOwned').alias('NumberCarsOwned'),
        F.col('pa.AddressLine1').alias('AddressLine1'),
        F.col('pa.AddressLine2').alias('AddressLine2'),
        F.col('pph.PhoneNumber').alias('Phone'),
        F.col('spd.DateFirstPurchase').alias('DateFirstPurchase'),
        F.lit(None).alias('CommuteDistance')
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upserting data to gold layer:

# COMMAND ----------

# Upserting data to gold layer
upsert_delta_table(
  df_source_table=df_DimCustomer,
  sink_table_name=gold_target_table_name,
  primary_keys=primary_keys,
)
