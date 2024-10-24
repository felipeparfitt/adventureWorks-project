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
table_name = "SpecialOffer"
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
    StructField("SpecialOfferID", IntegerType(), False),
    StructField("Description", StringType(), False),
    StructField("DiscountPct", DecimalType(10,4), False),
    StructField("Type", StringType(), False),
    StructField("Category", StringType(), False),
    StructField("StartDate", TimestampType(), False),
    StructField("EndDate", TimestampType(), False),
    StructField("MinQty", IntegerType(), False),
    StructField("MaxQty", IntegerType(), True),
    StructField("rowguid", StringType(), False),
    StructField("ModifiedDate", TimestampType(), False),
    StructField("_process_timestamp", TimestampType(), False),
    StructField("_input_file_name", StringType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the silver table

# COMMAND ----------

# Creating the Sales_SpecialOffer table in silver layer
spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {silver_target_table_name} (
        SpecialOfferID INT NOT NULL PRIMARY KEY,
        Description STRING NOT NULL,
        DiscountPct DECIMAL(10, 4) NOT NULL DEFAULT 0.00,
        Type STRING NOT NULL,
        Category STRING NOT NULL,
        StartDate TIMESTAMP NOT NULL,
        EndDate TIMESTAMP NOT NULL,
        MinQty INT NOT NULL DEFAULT 0,
        MaxQty INT,
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
        "ADD CONSTRAINT CK_SpecialOffer_DiscountPct CHECK (DiscountPct >= 0.00)",
        "ADD CONSTRAINT CK_SpecialOffer_EndDate CHECK (EndDate >= StartDate)",
        "ADD CONSTRAINT CK_SpecialOffer_MaxQty CHECK (MaxQty >= 0 OR MaxQty IS NULL)",
        "ADD CONSTRAINT CK_SpecialOffer_MinQty CHECK (MinQty >= 0)"
    ]

# Call the function to add the constraints
add_constraints(table_name=silver_target_table_name, constraints=constraints)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming Function

# COMMAND ----------

def tranforming_SpecialOffer(
    df_SpecialOffer: DataFrame,
    sink_table_name: str,
    primary_keys: list,
    expected_schema: StructType
    ) -> DataFrame:
    """
    Transforms and cleans data from the Sales.SpecialOffer table.

    Parameters:
        df_SpecialOffer (DataFrame): The DataFrame containing the SpecialOffer data to be transformed.
        sink_table_name (str): The name of the Delta table where the transformed data will be checked against constraints.
        primary_keys (list): A list of column names that serve as primary keys for deduplication.
        expected_schema (StructType): The expected schema for the transformed DataFrame.    
    Returns:
        DataFrame: The cleaned and transformed DataFrame with verified schema, ready for further processing.
    """
    print(f"Transforming the adventure_dev.bronze.sales_SpecialOffer: ", end='')

    # Removing duplicates through primary keys
    df_SpecialOffer_dedup = df_deduplicate(
        df=df_SpecialOffer, 
        primary_keys=primary_keys,
        order_col='ModifiedDate'
    )
    # Setting default values to null values
    df_SpecialOffer_fillna = df_SpecialOffer_dedup.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp())
        .otherwise(F.col('ModifiedDate'))
    )
    df_SpecialOffer_fillna = df_SpecialOffer_fillna.withColumn(
        '_process_timestamp',
        F.when(F.col('_process_timestamp').isNull(), F.current_timestamp())
        .otherwise(F.col('_process_timestamp'))
    )
    df_SpecialOffer_fillna = df_SpecialOffer_fillna.fillna(
        0, 
        subset=['DiscountPct']
    )
    df_SpecialOffer_fillna = df_SpecialOffer_fillna.fillna(
        0, 
        subset=['MinQty']
    )
    df_SpecialOffer_fillna = df_SpecialOffer_fillna.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr("uuid()"))
        .otherwise(F.col('rowguid'))
    )

    # Checking integrity of the data
    constrains_conditions = get_table_constraints_conditions(sink_table_name)
    df_SpecialOffer_cr = df_SpecialOffer_fillna.filter(F.expr(constrains_conditions))
    df_SpecialOffer_qr = df_SpecialOffer_fillna.filter(~F.expr(constrains_conditions))

    # Casting to expected schema
    select_exprs = [
      F.col(field.name).cast(field.dataType).alias(field.name)
      for field in expected_schema.fields
    ]
    df_SpecialOffer_casted = df_SpecialOffer_cr.select(*select_exprs)
    
    # Verify expected schema
    verify_schema(df_SpecialOffer_casted, expected_schema)

    print("Success !!")
    print("*******************************")

    return df_SpecialOffer_casted

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Reading/Transforming/Writing the table from bronze to silver layer

# COMMAND ----------

# # Reading data from bronze layer
print(f"Reading {bronze_source_table_name}: ", end='')
df_SpecialOffer_bronze = spark.read.format("delta").table(bronze_source_table_name)
print("Success!!")
print("*******************************")

# Tranforming bronze layer
df_SpecialOffer_transformed = tranforming_SpecialOffer(
    df_SpecialOffer=df_SpecialOffer_bronze,
    sink_table_name=silver_target_table_name,
    primary_keys=primary_keys,
    expected_schema=expected_schema
)

# Upserting data to silver layer
upsert_delta_table(
  df_source_table=df_SpecialOffer_transformed,
  sink_table_name=silver_target_table_name,
  primary_keys=primary_keys
)
