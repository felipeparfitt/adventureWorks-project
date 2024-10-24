# Databricks notebook source
# MAGIC %md
# MAGIC # ETL: Bronze to Silver Layer
# MAGIC
# MAGIC This notebook provides functions to handle reading, transforming, and writing data between the bronze and silver layers in batch mode.
# MAGIC
# MAGIC ## Batch Mode
# MAGIC - **Reading Data from Bronze:** Reads data from a specified table in the bronze layer of a given environment. It returns the DataFrame for further processing, ensuring the data is successfully retrieved.
# MAGIC
# MAGIC - **Transforming Data:** Transforms data from the bronze layer by removing duplicates, filling missing values, applying data integrity constraints, and casting to the expected schema.
# MAGIC
# MAGIC - **Upserting Data to Silver:** Upserts data from a source DataFrame into a Delta table in the silver layer, updating existing records and inserting new ones based on primary keys.

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
table_name = "vPersonDemographics"
schema_name = adventureworks_tables_info[table_name]['schema_name']
schema_table_name = f"{schema_name}_{table_name}"
primary_keys = adventureworks_tables_info[table_name]['primary_keys']

# Source info
bronze_source_path = f"{bronze_path}/{schema_name}/{table_name}"
bronze_source_table_name = f"{catalog_name}.bronze.{schema_table_name}"

# Target info
silver_target_path = f"{silver_path}/{schema_name}/{table_name}"
silver_target_table_name = f"{catalog_name}.silver.{schema_table_name}"

# COMMAND ----------

# Expected schema
expected_schema = StructType([
    StructField("BusinessEntityID", IntegerType(), False),
    StructField("TotalPurchaseYTD", DecimalType(19, 4), False),
    StructField("DateFirstPurchase", TimestampType(), False),
    StructField("BirthDate", TimestampType(), False),
    StructField("MaritalStatus", StringType(), False),
    StructField("YearlyIncome", StringType(), False),
    StructField("Gender", StringType(), False),
    StructField("TotalChildren", IntegerType(), False),
    StructField("NumberChildrenAtHome", IntegerType(), False),
    StructField("Education", StringType(), False),
    StructField("Occupation", StringType(), False),
    StructField("HomeOwnerFlag", BooleanType(), False),
    StructField("NumberCarsOwned", IntegerType(), False),
    StructField("_process_timestamp", TimestampType(), False),
    StructField("_input_file_name", StringType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the silver table

# COMMAND ----------

# Creating the vPersonDemographics table in silver layer
spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {silver_target_table_name} (
        BusinessEntityID INT NOT NULL PRIMARY KEY,
        TotalPurchaseYTD DECIMAL(19,4) NOT NULL,
        DateFirstPurchase TIMESTAMP NOT NULL,
        BirthDate TIMESTAMP NOT NULL,
        MaritalStatus STRING NOT NULL,
        YearlyIncome STRING NOT NULL,
        Gender STRING NOT NULL,
        TotalChildren INT NOT NULL,
        NumberChildrenAtHome INT NOT NULL,
        Education STRING NOT NULL,
        Occupation STRING NOT NULL,
        HomeOwnerFlag BOOLEAN NOT NULL,
        NumberCarsOwned INT NOT NULL,
        _process_timestamp TIMESTAMP NOT NULL DEFAULT current_timestamp(),
        _input_file_name STRING NOT NULL
    )
    USING DELTA
    LOCATION '{silver_target_path}'
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')
""")
constraints = [
    "ADD CONSTRAINT CK_vPersonDemographics_BusinessEntityID CHECK (BusinessEntityID IS NOT NULL)",
    "ADD CONSTRAINT CK_vPersonDemographics_TotalPurchaseYTD CHECK (TotalPurchaseYTD IS NOT NULL)",
    "ADD CONSTRAINT CK_vPersonDemographics_DateFirstPurchase CHECK (DateFirstPurchase IS NOT NULL)",
    "ADD CONSTRAINT CK_vPersonDemographics_BirthDate CHECK (BirthDate IS NOT NULL)",
    "ADD CONSTRAINT CK_vPersonDemographics_MaritalStatus CHECK (MaritalStatus IS NOT NULL)",
    "ADD CONSTRAINT CK_vPersonDemographics_YearlyIncome CHECK (YearlyIncome IS NOT NULL)",
    "ADD CONSTRAINT CK_vPersonDemographics_Gender CHECK (Gender IS NOT NULL)",
    "ADD CONSTRAINT CK_vPersonDemographics_TotalChildren CHECK (TotalChildren IS NOT NULL)",
    "ADD CONSTRAINT CK_vPersonDemographics_NumberChildrenAtHome CHECK (NumberChildrenAtHome IS NOT NULL)",
    "ADD CONSTRAINT CK_vPersonDemographics_Education CHECK (Education IS NOT NULL)",
    "ADD CONSTRAINT CK_vPersonDemographics_Occupation CHECK (Occupation IS NOT NULL)",
    "ADD CONSTRAINT CK_vPersonDemographics_HomeOwnerFlag CHECK (HomeOwnerFlag IS NOT NULL)",
    "ADD CONSTRAINT CK_vPersonDemographics_NumberCarsOwned CHECK (NumberCarsOwned IS NOT NULL)"
]

# Call the function to add the constraints
add_constraints(table_name=silver_target_table_name, constraints=constraints)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming Function

# COMMAND ----------

def tranforming_vPersonDemographics(
    df_vPersonDemographics: DataFrame,
    sink_table_name: str,
    primary_keys: list,
    expected_schema: StructType
    ) -> DataFrame:
    """
    Transforms and cleans data from the Sales.vPersonDemographics table.

    Parameters:
        df_vPersonDemographics (DataFrame): The DataFrame containing the vPersonDemographics data to be transformed.
        sink_table_name (str): The name of the Delta table where the transformed data will be checked against constraints.
        primary_keys (list): A list of column names that serve as primary keys for deduplication.
        expected_schema (StructType): The expected schema for the transformed DataFrame.    
    Returns:
        DataFrame: The cleaned and transformed DataFrame with verified schema, ready for further processing.
    """
    print(f"Transforming the adventure_dev.bronze.sales_vPersonDemographics: ", end='')

    # Removing duplicates through primary keys
    df_vPersonDemographics_dedup = df_deduplicate(
        df=df_vPersonDemographics, 
        primary_keys=primary_keys,
        order_col='_process_timestamp'
    )
    # Setting default values to null values
    df_vPersonDemographics_fillna = df_vPersonDemographics_dedup.withColumn(
        '_process_timestamp',
        F.when(F.col('_process_timestamp').isNull(), F.current_timestamp())
        .otherwise(F.col('_process_timestamp'))
    )

    # Checking integrity of the data
    constrains_conditions = get_table_constraints_conditions(sink_table_name)
    df_vPersonDemographics_cr = df_vPersonDemographics_fillna.filter(F.expr(constrains_conditions))
    df_vPersonDemographics_qr = df_vPersonDemographics_fillna.filter(~F.expr(constrains_conditions))

    # Casting to expected schema
    select_exprs = [
      F.col(field.name).cast(field.dataType).alias(field.name)
      for field in expected_schema.fields
    ]
    df_vPersonDemographics_casted = df_vPersonDemographics_cr.select(*select_exprs)
    
    # Verify expected schema
    verify_schema(df_vPersonDemographics_casted, expected_schema)

    print("Success !!")
    print("*******************************")

    return df_vPersonDemographics_casted

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Reading/Transforming/Writing the table from bronze to silver layer

# COMMAND ----------

# # Reading data from bronze layer
print(f"Reading {bronze_source_table_name}: ", end='')
df_vPersonDemographics_bronze = spark.read.format("delta").table(bronze_source_table_name)
print("Success!!")
print("*******************************")

# Tranforming bronze layer
df_vPersonDemographics_transformed = tranforming_vPersonDemographics(
    df_vPersonDemographics=df_vPersonDemographics_bronze,
    sink_table_name=silver_target_table_name,
    primary_keys=primary_keys,
    expected_schema=expected_schema
)

# Upserting data to silver layer
upsert_delta_table(
  df_source_table=df_vPersonDemographics_transformed,
  sink_table_name=silver_target_table_name,
  primary_keys=primary_keys
)
