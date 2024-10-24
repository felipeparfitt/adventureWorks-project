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
table_name = "Product"
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
    StructField("ProductID", IntegerType(), False),
    StructField("Name", StringType(), False),
    StructField("ProductNumber", StringType(), False),
    StructField("MakeFlag", BooleanType(), False),
    StructField("FinishedGoodsFlag", BooleanType(), False),
    StructField("Color", StringType(), True),
    StructField("SafetyStockLevel", ShortType(), False),
    StructField("ReorderPoint", ShortType(), False),
    StructField("StandardCost", DecimalType(19, 4), False),
    StructField("ListPrice", DecimalType(19, 4), False),
    StructField("Size", StringType(), True),
    StructField("SizeUnitMeasureCode", StringType(), True),
    StructField("WeightUnitMeasureCode", StringType(), True),
    StructField("Weight", DecimalType(8, 2), True),
    StructField("DaysToManufacture", IntegerType(), False),
    StructField("ProductLine", StringType(), True),
    StructField("Class", StringType(), True),
    StructField("Style", StringType(), True),
    StructField("ProductSubcategoryID", IntegerType(), True),
    StructField("ProductModelID", IntegerType(), True),
    StructField("SellStartDate", TimestampType(), False),
    StructField("SellEndDate", TimestampType(), True),
    StructField("DiscontinuedDate", TimestampType(), True),
    StructField("rowguid", StringType(), False),
    StructField("ModifiedDate", TimestampType(), False),
    StructField("_process_timestamp", TimestampType(), False),
    StructField("_input_file_name", StringType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the silver table

# COMMAND ----------

# Creating the Production_Product table in silver layer
spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {silver_target_table_name} (
        ProductID INT NOT NULL PRIMARY KEY,
        Name STRING NOT NULL,
        ProductNumber STRING NOT NULL,
        MakeFlag BOOLEAN NOT NULL DEFAULT true,
        FinishedGoodsFlag BOOLEAN NOT NULL DEFAULT true,
        Color STRING,
        SafetyStockLevel SMALLINT NOT NULL,
        ReorderPoint SMALLINT NOT NULL,
        StandardCost DECIMAL(19, 4) NOT NULL,
        ListPrice DECIMAL(19, 4) NOT NULL,
        Size STRING,
        SizeUnitMeasureCode STRING,
        WeightUnitMeasureCode STRING,
        Weight DECIMAL(8, 2),
        DaysToManufacture INT NOT NULL,
        ProductLine STRING,
        Class STRING,
        Style STRING,
        ProductSubcategoryID INT,
        ProductModelID INT,
        SellStartDate TIMESTAMP NOT NULL,
        SellEndDate TIMESTAMP,
        DiscontinuedDate TIMESTAMP,
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
        "ADD CONSTRAINT CK_Product_DaysToManufacture CHECK (DaysToManufacture >= 0)",
        "ADD CONSTRAINT CK_Product_ListPrice CHECK (ListPrice >= 0.00)",
        "ADD CONSTRAINT CK_Product_ReorderPoint CHECK (ReorderPoint > 0)",
        "ADD CONSTRAINT CK_Product_SafetyStockLevel CHECK (SafetyStockLevel > 0)",
        "ADD CONSTRAINT CK_Product_StandardCost CHECK (StandardCost >= 0.00)",
        "ADD CONSTRAINT CK_Product_Weight CHECK (Weight >= 0.00 OR Weight IS NULL)",
        "ADD CONSTRAINT CK_Product_Class CHECK (upper(Class) IN ('H', 'M', 'L') OR Class IS NULL)",
        "ADD CONSTRAINT CK_Product_ProductLine CHECK (upper(ProductLine) IN ('R', 'M', 'T', 'S') OR ProductLine IS NULL)",
        "ADD CONSTRAINT CK_Product_SellEndDate CHECK (SellEndDate >= SellStartDate OR SellEndDate IS NULL)",
        "ADD CONSTRAINT CK_Product_Style CHECK (upper(Style) IN ('U', 'M', 'W') OR Style IS NULL)",
    ]
# Modified conditions:
# CK_Product_Weight CHECK -> (Weight >= 0.00)" to (Weight >= 0.00 OR Weight IS NULL)"

# Call the function to add the constraints
add_constraints(table_name=silver_target_table_name, constraints=constraints)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming Function

# COMMAND ----------

def tranforming_Product(
    df_Product: DataFrame,
    sink_table_name: str,
    primary_keys: list,
    expected_schema: StructType
    ) -> DataFrame:
    """
    Transforms and cleans data from the Person.Product table.

    Parameters:
        df_Product (DataFrame): The DataFrame containing the Product data to be transformed.
        sink_table_name (str): The name of the Delta table where the transformed data will be checked against constraints.
        primary_keys (list): A list of column names that serve as primary keys for deduplication.
        expected_schema (StructType): The expected schema for the transformed DataFrame.    
    Returns:
        DataFrame: The cleaned and transformed DataFrame with verified schema, ready for further processing.
    """
    print(f"Transforming the adventure_dev.bronze.production_product: ", end='')

    # Removing duplicates through primary keys
    df_Product_dedup = df_deduplicate(
        df=df_Product, 
        primary_keys=primary_keys,
        order_col='ModifiedDate'
    )
    # Setting default values to null values
    df_Product_fillna = df_Product_dedup.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp())
        .otherwise(F.col('ModifiedDate'))
    )
    df_Product_fillna = df_Product_fillna.withColumn(
        '_process_timestamp',
        F.when(F.col('_process_timestamp').isNull(), F.current_timestamp())
        .otherwise(F.col('_process_timestamp'))
    )
    df_Product_fillna = df_Product_fillna.withColumn(
        'MakeFlag',
        F.when(F.col('MakeFlag').isNull(), True)
        .otherwise(F.col('MakeFlag'))
    )
    df_Product_fillna = df_Product_fillna.withColumn(
        'FinishedGoodsFlag',
        F.when(F.col('FinishedGoodsFlag').isNull(), True)
        .otherwise(F.col('FinishedGoodsFlag'))
    )
    df_Product_fillna = df_Product_fillna.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr("uuid()"))
        .otherwise(F.col('rowguid'))
    )

    # Removing whitespaces
    df_Product_fillna = df_Product_fillna.withColumn('Class', F.trim(F.col('Class')))
    df_Product_fillna = df_Product_fillna.withColumn('Style', F.trim(F.col('Style')))
    df_Product_fillna = df_Product_fillna.withColumn('ProductLine', F.trim(F.col('ProductLine')))

    # Checking integrity of the data
    constrains_conditions = get_table_constraints_conditions(sink_table_name)
    df_Product_cr = df_Product_fillna.filter(F.expr(constrains_conditions))
    df_Product_qr = df_Product_fillna.filter(~F.expr(constrains_conditions))
    
    # Casting to expected schema
    select_exprs = [
      F.col(field.name).cast(field.dataType).alias(field.name)
      for field in expected_schema.fields
    ]
    df_Product_casted = df_Product_cr.select(*select_exprs)
    
    # Verify expected schema
    verify_schema(df_Product_casted, expected_schema)

    print("Success !!")
    print("*******************************")

    return df_Product_casted

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Reading/Transforming/Writing the table from bronze to silver layer

# COMMAND ----------

# # Reading data from bronze layer
print(f"Reading {bronze_source_table_name}: ", end='')
df_Product_bronze = spark.read.format("delta").table(bronze_source_table_name)
print("Success!!")
print("*******************************")

# Tranforming bronze layer
df_Product_transformed = tranforming_Product(
    df_Product=df_Product_bronze,
    sink_table_name=silver_target_table_name,
    primary_keys=primary_keys,
    expected_schema=expected_schema
)

# Upserting data to silver layer
upsert_delta_table(
  df_source_table=df_Product_transformed,
  sink_table_name=silver_target_table_name,
  primary_keys=primary_keys
)
