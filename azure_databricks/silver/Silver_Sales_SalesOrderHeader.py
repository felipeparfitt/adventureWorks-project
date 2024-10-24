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
table_name = "SalesOrderHeader"
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
    StructField("SalesOrderID", IntegerType(), False),
    StructField("RevisionNumber", ByteType(), False),
    StructField("OrderDate", TimestampType(), False),
    StructField("DueDate", TimestampType(), False),
    StructField("ShipDate", TimestampType(), True),
    StructField("Status", ByteType(), False),
    StructField("OnlineOrderFlag", BooleanType(), False),
    StructField("SalesOrderNumber", StringType(), True),
    StructField("PurchaseOrderNumber", StringType(), True),
    StructField("AccountNumber", StringType(), True),
    StructField("CustomerID", IntegerType(), False),
    StructField("SalesPersonID", IntegerType(), True),
    StructField("TerritoryID", IntegerType(), True),
    StructField("BillToAddressID", IntegerType(), False),
    StructField("ShipToAddressID", IntegerType(), False),
    StructField("ShipMethodID", IntegerType(), False),
    StructField("CreditCardID", IntegerType(), True),
    StructField("CreditCardApprovalCode", StringType(), True),
    StructField("CurrencyRateID", IntegerType(), True),
    StructField("SubTotal", DecimalType(19, 4), False),
    StructField("TaxAmt", DecimalType(19, 4), False),
    StructField("Freight", DecimalType(19, 4), False),
    StructField("TotalDue", DecimalType(19, 4), True),
    StructField("Comment", StringType(), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", TimestampType(), False),
    StructField("_process_timestamp", TimestampType(), False),
    StructField("_input_file_name", StringType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the silver table

# COMMAND ----------

# Creating the sales_SalesOrderHeader table in silver layer
spark.sql(f"""
      CREATE EXTERNAL TABLE IF NOT EXISTS {silver_target_table_name} (
          SalesOrderID INT NOT NULL PRIMARY KEY,
          RevisionNumber TINYINT NOT NULL DEFAULT 0,
          OrderDate TIMESTAMP NOT NULL DEFAULT current_timestamp(),
          DueDate TIMESTAMP NOT NULL,
          ShipDate TIMESTAMP,
          Status TINYINT NOT NULL DEFAULT 1,
          OnlineOrderFlag BOOLEAN NOT NULL DEFAULT true,
          SalesOrderNumber STRING,
          PurchaseOrderNumber STRING,
          AccountNumber STRING,
          CustomerID INT NOT NULL,
          SalesPersonID INT,
          TerritoryID INT,
          BillToAddressID INT NOT NULL,
          ShipToAddressID INT NOT NULL,
          ShipMethodID INT NOT NULL,
          CreditCardID INT,
          CreditCardApprovalCode STRING,
          CurrencyRateID INT,
          SubTotal DECIMAL(19, 4) NOT NULL DEFAULT 0,
          TaxAmt DECIMAL(19, 4) NOT NULL DEFAULT 0,
          Freight DECIMAL(19, 4) NOT NULL DEFAULT 0,
          TotalDue DECIMAL(19, 4),
          Comment STRING,
          rowguid STRING,
          ModifiedDate TIMESTAMP NOT NULL DEFAULT current_timestamp(),
          _process_timestamp TIMESTAMP NOT NULL DEFAULT current_timestamp(),
          _input_file_name STRING NOT NULL
      )
      USING DELTA 
      LOCATION '{silver_target_path}'
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')
    """
)
constraints = [
        "ADD CONSTRAINT CK_SalesOrderHeader_DueDate CHECK (DueDate >= OrderDate)",
        "ADD CONSTRAINT CK_SalesOrderHeader_Freight CHECK (Freight >= 0.00)",
        "ADD CONSTRAINT CK_SalesOrderHeader_ShipDate CHECK (ShipDate >= OrderDate OR ShipDate IS NULL)",
        "ADD CONSTRAINT CK_SalesOrderHeader_Status CHECK (Status >= 0 AND Status <= 8)",
        "ADD CONSTRAINT CK_SalesOrderHeader_SubTotal CHECK (SubTotal >= 0.00)",
        "ADD CONSTRAINT CK_SalesOrderHeader_TaxAmt CHECK (TaxAmt >= 0.00)"
    ]

# Call the function to add the constraints
add_constraints(table_name=silver_target_table_name, constraints=constraints)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming Function

# COMMAND ----------

def tranforming_salesOrderHeader(
    df_salesOrderHeader: DataFrame,
    sink_table_name: str,
    primary_keys: list,
    expected_schema: StructType
    ) -> DataFrame:
    """
    Transforms and cleans data from the SalesOrderHeader DataFrame.

    Parameters:
        df_salesOrderHeader (DataFrame): The DataFrame containing the SalesOrderHeader data to be transformed.
        sink_table_name (str): The name of the Delta table where the transformed data will be checked against constraints.
        primary_keys (list): A list of column names that serve as primary keys for deduplication.

    Returns:
        DataFrame: The cleaned and transformed DataFrame with verified schema, ready for further processing.
    """
    print(f"Transforming the adventure_dev.bronze.SalesOrderHeader: ", end='')

    # Removing duplicates through primary keys
    df_salesOrderHeader_dedup = df_deduplicate(df_salesOrderHeader, primary_keys, 'ModifiedDate')

    # Setting default values to null values
    df_salesOrderHeader_fillna = df_salesOrderHeader_dedup.fillna(0, subset=['RevisionNumber', 'SubTotal', 'TaxAmt', 'Freight'])
    df_salesOrderHeader_fillna = df_salesOrderHeader_fillna.fillna(1, subset=['Status'])
    df_salesOrderHeader_fillna = df_salesOrderHeader_fillna.fillna(True, subset=['OnlineOrderFlag'])
    df_salesOrderHeader_fillna = df_salesOrderHeader_fillna.withColumn(
      'OrderDate', 
      F.when(F.col('OrderDate').isNull(), F.current_timestamp())
      .otherwise(F.col('OrderDate'))
    )
    df_salesOrderHeader_fillna = df_salesOrderHeader_fillna.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp())
        .otherwise(F.col('ModifiedDate'))
    )
    df_salesOrderHeader_fillna = df_salesOrderHeader_fillna.withColumn(
        '_process_timestamp',
        F.when(F.col('_process_timestamp').isNull(), F.current_timestamp())
        .otherwise(F.col('_process_timestamp'))
    )
    df_salesOrderHeader_fillna = df_salesOrderHeader_fillna.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr("uuid()"))
        .otherwise(F.col('rowguid'))
    )

    df_salesOrderHeader_fillna = df_salesOrderHeader_fillna.withColumn(
        'TotalDue',  
        F.when(
            (F.col('SubTotal') + F.col('TaxAmt') + F.col('Freight')).isNull(), 
            0
        )
        .otherwise(F.col('TotalDue'))
    )

    # Checking integrity of the data
    constrains_conditions = get_table_constraints_conditions(sink_table_name)
    df_salesOrderHeader_cr = df_salesOrderHeader_fillna.filter(F.expr(constrains_conditions))
    df_salesOrderHeader_qr = df_salesOrderHeader_fillna.filter(~F.expr(constrains_conditions))

    # Casting to expected schema
    select_exprs = [
      F.col(field.name).cast(field.dataType).alias(field.name)
      for field in expected_schema.fields
    ]
    df_salesOrderHeader_casted = df_salesOrderHeader_cr.select(*select_exprs)
    
    # Verify expected schema
    verify_schema(df_salesOrderHeader_casted, expected_schema)

    print("Success !!")
    print("*******************************")

    return df_salesOrderHeader_casted

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Reading/Transforming/Writing the table from bronze to silver layer

# COMMAND ----------

# # Reading data from bronze layer
print(f"Reading {bronze_source_table_name}: ", end='')
df_salesOrderHeader_bronze = spark.read.format("delta").table(bronze_source_table_name)
print("Success!!")
print("*******************************")

# Tranforming bronze layer
df_salesOrderHeader_transformed = tranforming_salesOrderHeader(
    df_salesOrderHeader=df_salesOrderHeader_bronze,
    sink_table_name=silver_target_table_name,
    primary_keys=primary_keys,
    expected_schema=expected_schema
)

# Upserting data to silver layer
upsert_delta_table(
  df_source_table=df_salesOrderHeader_transformed,
  sink_table_name=silver_target_table_name,
  primary_keys=primary_keys
)
