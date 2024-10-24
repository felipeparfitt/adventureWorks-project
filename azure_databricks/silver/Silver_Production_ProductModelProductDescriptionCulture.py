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
table_name = "ProductModelProductDescriptionCulture"
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
    StructField("ProductModelID", IntegerType(), False),
    StructField("ProductDescriptionID", IntegerType(), False),
    StructField("CultureID", StringType(), False),
    StructField("ModifiedDate", TimestampType(), False),
    StructField("_process_timestamp", TimestampType(), False),
    StructField("_input_file_name", StringType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the silver table

# COMMAND ----------

# Creating the Production_ProductModelProductDescriptionCulture table in silver layer
spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {silver_target_table_name} (
        ProductModelID INT NOT NULL PRIMARY KEY,
        ProductDescriptionID INT NOT NULL,
        CultureID STRING NOT NULL,
        ModifiedDate TIMESTAMP NOT NULL DEFAULT current_timestamp(),
        _process_timestamp TIMESTAMP NOT NULL DEFAULT current_timestamp(),
        _input_file_name STRING NOT NULL
    )
    USING DELTA
    LOCATION '{silver_target_path}'
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming Function

# COMMAND ----------

def tranforming_ProductModelProductDescriptionCulture(
    df_ProductModelProductDescriptionCulture: DataFrame,
    sink_table_name: str,
    primary_keys: list,
    expected_schema: StructType
    ) -> DataFrame:
    """
    Transforms and cleans data from the Person.ProductModelProductDescriptionCulture table.

    Parameters:
        df_ProductModelProductDescriptionCulture (DataFrame): The DataFrame containing the ProductModelProductDescriptionCulture data to be transformed.
        sink_table_name (str): The name of the Delta table where the transformed data will be checked against constraints.
        primary_keys (list): A list of column names that serve as primary keys for deduplication.
        expected_schema (StructType): The expected schema for the transformed DataFrame.    
    Returns:
        DataFrame: The cleaned and transformed DataFrame with verified schema, ready for further processing.
    """
    print(f"Transforming the adventure_dev.bronze.ProductModelProductDescriptionCulture: ", end='')

    # Removing duplicates through primary keys
    df_ProductModelProductDescriptionCulture_dedup = df_deduplicate(
        df=df_ProductModelProductDescriptionCulture, 
        primary_keys=primary_keys,
        order_col='ModifiedDate'
    )
    # Setting default values to null values
    df_ProductModelProductDescriptionCulture_fillna = df_ProductModelProductDescriptionCulture_dedup.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp())
        .otherwise(F.col('ModifiedDate'))
    )
    df_ProductModelProductDescriptionCulture_fillna = df_ProductModelProductDescriptionCulture_fillna.withColumn(
        '_process_timestamp',
        F.when(F.col('_process_timestamp').isNull(), F.current_timestamp())
        .otherwise(F.col('_process_timestamp'))
    )

    # Checking integrity of the data
    constrains_conditions = get_table_constraints_conditions(sink_table_name)
    df_ProductModelProductDescriptionCulture_cr = df_ProductModelProductDescriptionCulture_fillna.filter(F.expr(constrains_conditions))
    df_ProductModelProductDescriptionCulture_qr = df_ProductModelProductDescriptionCulture_fillna.filter(~F.expr(constrains_conditions))

    # Casting to expected schema
    select_exprs = [
      F.col(field.name).cast(field.dataType).alias(field.name)
      for field in expected_schema.fields
    ]
    df_ProductModelProductDescriptionCulture_casted = df_ProductModelProductDescriptionCulture_cr.select(*select_exprs)
    
    # Verify expected schema
    verify_schema(df_ProductModelProductDescriptionCulture_casted, expected_schema)

    print("Success !!")
    print("*******************************")

    return df_ProductModelProductDescriptionCulture_casted

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Reading/Transforming/Writing the table from bronze to silver layer

# COMMAND ----------

# # Reading data from bronze layer
print(f"Reading {bronze_source_table_name}: ", end='')
df_ProductModelProductDescriptionCulture_bronze = spark.read.format("delta").table(bronze_source_table_name)
print("Success!!")
print("*******************************")

# Tranforming bronze layer
df_ProductModelProductDescriptionCulture_transformed = tranforming_ProductModelProductDescriptionCulture(
    df_ProductModelProductDescriptionCulture=df_ProductModelProductDescriptionCulture_bronze,
    sink_table_name=silver_target_table_name,
    primary_keys=primary_keys,
    expected_schema=expected_schema
)

# Upserting data to silver layer
upsert_delta_table(
  df_source_table=df_ProductModelProductDescriptionCulture_transformed,
  sink_table_name=silver_target_table_name,
  primary_keys=primary_keys
)
