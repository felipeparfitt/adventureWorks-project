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
table_name = "SalesTerritory"
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
    StructField("TerritoryID", IntegerType(), False),
    StructField("Name", StringType(), False),
    StructField("CountryRegionCode", StringType(), False),
    StructField("Group", StringType(), False),
    StructField("SalesYTD", DecimalType(19,4), False),
    StructField("SalesLastYear", DecimalType(19,4), False),
    StructField("CostYTD", DecimalType(19,4), False),
    StructField("CostLastYear", DecimalType(19,4), False),
    StructField("rowguid", StringType(), False),
    StructField("ModifiedDate", TimestampType(), False),
    StructField("_process_timestamp", TimestampType(), False),
    StructField("_input_file_name", StringType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the silver table

# COMMAND ----------

# Creating the Sales_SalesTerritory table in silver layer
spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {silver_target_table_name} (
        TerritoryID INT NOT NULL PRIMARY KEY,
        Name STRING NOT NULL,
        CountryRegionCode STRING NOT NULL,
        Group STRING NOT NULL,
        SalesYTD DECIMAL(19, 4) NOT NULL DEFAULT 0.00,
        SalesLastYear DECIMAL(19, 4) NOT NULL DEFAULT 0.00,
        CostYTD DECIMAL(19, 4) NOT NULL DEFAULT 0.00,
        CostLastYear DECIMAL(19, 4) NOT NULL DEFAULT 0.00,
        rowguid STRING NOT NULL,
        ModifiedDate TIMESTAMP NOT NULL DEFAULT current_timestamp(),
        _process_timestamp TIMESTAMP NOT NULL DEFAULT current_timestamp(),
        _input_file_name STRING NOT NULL
    )
    USING DELTA
    LOCATION '{silver_target_path}'
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')
""")
constraints = [
        "ADD CONSTRAINT CK_SalesTerritory_CostLastYear CHECK (CostLastYear >= 0.00)",
        "ADD CONSTRAINT CK_SalesTerritory_CostYTD CHECK (CostYTD >= 0.00)",
        "ADD CONSTRAINT CK_SalesTerritory_SalesLastYear CHECK (SalesLastYear >= 0.00)",
        "ADD CONSTRAINT CK_SalesTerritory_SalesYTD CHECK (SalesYTD >= 0.00)"
    ]

# Call the function to add the constraints
add_constraints(table_name=silver_target_table_name, constraints=constraints)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming Function

# COMMAND ----------

def tranforming_SalesTerritory(
    df_SalesTerritory: DataFrame,
    sink_table_name: str,
    primary_keys: list,
    expected_schema: StructType
    ) -> DataFrame:
    """
    Transforms and cleans data from the Sales.SalesTerritory table.

    Parameters:
        df_SalesTerritory (DataFrame): The DataFrame containing the SalesTerritory data to be transformed.
        sink_table_name (str): The name of the Delta table where the transformed data will be checked against constraints.
        primary_keys (list): A list of column names that serve as primary keys for deduplication.
        expected_schema (StructType): The expected schema for the transformed DataFrame.    
    Returns:
        DataFrame: The cleaned and transformed DataFrame with verified schema, ready for further processing.
    """
    print(f"Transforming the adventure_dev.bronze.sales_SalesTerritory: ", end='')

    # Removing duplicates through primary keys
    df_SalesTerritory_dedup = df_deduplicate(
        df=df_SalesTerritory, 
        primary_keys=primary_keys,
        order_col='ModifiedDate'
    )
    # Setting default values to null values
    df_SalesTerritory_fillna = df_SalesTerritory_dedup.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp())
        .otherwise(F.col('ModifiedDate'))
    )
    df_SalesTerritory_fillna = df_SalesTerritory_fillna.withColumn(
        '_process_timestamp',
        F.when(F.col('_process_timestamp').isNull(), F.current_timestamp())
        .otherwise(F.col('_process_timestamp'))
    )
    df_SalesTerritory_fillna = df_SalesTerritory_fillna.fillna(
        0.00, 
        subset=['SalesYTD', 'SalesLastYear', 'CostYTD', 'CostLastYear']
    )
    df_SalesTerritory_fillna = df_SalesTerritory_fillna.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr("uuid()"))
        .otherwise(F.col('rowguid'))
    )


    # Checking integrity of the data
    constrains_conditions = get_table_constraints_conditions(sink_table_name)
    df_SalesTerritory_cr = df_SalesTerritory_fillna.filter(F.expr(constrains_conditions))
    df_SalesTerritory_qr = df_SalesTerritory_fillna.filter(~F.expr(constrains_conditions))

    # Casting to expected schema
    select_exprs = [
      F.col(field.name).cast(field.dataType).alias(field.name)
      for field in expected_schema.fields
    ]
    df_SalesTerritory_casted = df_SalesTerritory_cr.select(*select_exprs)
    
    # Verify expected schema
    verify_schema(df_SalesTerritory_casted, expected_schema)

    print("Success !!")
    print("*******************************")

    return df_SalesTerritory_casted

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Reading/Transforming/Writing the table from bronze to silver layer

# COMMAND ----------

# # Reading data from bronze layer
print(f"Reading {bronze_source_table_name}: ", end='')
df_SalesTerritory_bronze = spark.read.format("delta").table(bronze_source_table_name)
print("Success!!")
print("*******************************")

# Tranforming bronze layer
df_SalesTerritory_transformed = tranforming_SalesTerritory(
    df_SalesTerritory=df_SalesTerritory_bronze,
    sink_table_name=silver_target_table_name,
    primary_keys=primary_keys,
    expected_schema=expected_schema
)

# Upserting data to silver layer
upsert_delta_table(
  df_source_table=df_SalesTerritory_transformed,
  sink_table_name=silver_target_table_name,
  primary_keys=primary_keys
)
