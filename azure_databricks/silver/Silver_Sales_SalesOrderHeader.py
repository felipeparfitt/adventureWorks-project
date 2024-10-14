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

# sales_SalesOrderHeader expected schema
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
    print(constrains_conditions)
    print(f"# antes do filter: {df_salesOrderHeader_fillna.count()}")
    df_salesOrderHeader_cr = df_salesOrderHeader_fillna.filter(F.expr(constrains_conditions))
    print(f"# depois do filter: {df_salesOrderHeader_cr.count()}")
    df_salesOrderHeader_qr = df_salesOrderHeader_fillna.filter(~F.expr(constrains_conditions))
    print(f"# quarentine: {df_salesOrderHeader_qr.count()}")
    display(df_salesOrderHeader_qr)

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

# COMMAND ----------

from decimal import Decimal

#Definir o esquema da tabela (mesmo esquema da tabela original)
schema = StructType([
    StructField("SalesOrderID", IntegerType(), False),
    StructField("RevisionNumber", ByteType(), False),
    StructField("OrderDate", StringType(), False),
    StructField("DueDate", StringType(), False),
    StructField("ShipDate", StringType(), True),
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
    StructField("SubTotal", DecimalType(10, 1), False),
    StructField("TaxAmt", DecimalType(10, 1), False),
    StructField("Freight", DecimalType(10, 1), False),
    StructField("TotalDue", DecimalType(10, 1), True),
    StructField("Comment", StringType(), True),
    StructField("rowguid", StringType(), True),
    StructField("ModifiedDate", StringType(), False),
    StructField("_process_timestamp", StringType(), False),
    StructField("_input_file_name", StringType(), False)
])
columns = ["SalesOrderID", "RevisionNumber", "OrderDate", "DueDate", "ShipDate", "Status", 
           "OnlineOrderFlag", "SalesOrderNumber", "PurchaseOrderNumber", "AccountNumber", 
           "CustomerID", "SalesPersonID", "TerritoryID", "BillToAddressID", "ShipToAddressID", 
           "ShipMethodID", "CreditCardID", "CreditCardApprovalCode", "CurrencyRateID", 
           "SubTotal", "TaxAmt", "Freight", "TotalDue", "Comment", "rowguid", 
           "ModifiedDate", "_process_timestamp", "_input_file_name"]

# Dados fictícios para teste (2 válidos e 1 inválido por teste)
# Válidos 3
# Falhas nas condições específicas:
    # Fail: DueDate < OrderDate
    # Fail: Freight < 0
    # Fail: ShipDate < OrderDate
    # Fail: Status > 8
    # Fail: SubTotal < 0
    # Fail: TaxAmt < 0

data = [
    (1, 1, "2024-01-01 10:00:00", "2024-01-02 10:00:00", "2024-01-03 10:00:00", 5, True, "SO123", "PO123", "AN123", 1, None, None, 
     10, 20, 30, None, "123ABC", None, Decimal('100.00'), Decimal('10.00'), Decimal('20.00'), Decimal('130.00'), 
     "Sample comment", "UUID-123", "2024-01-01 10:00:00", "2024-01-01 10:00:00", "input_file_1"),
    (2, 1, "2024-01-02 10:00:00", "2024-01-03 10:00:00", None, 4, True, "SO124", "PO124", "AN124", 2, None, None, 
     15, 25, 35, None, "456DEF", None, Decimal('150.00'), Decimal('12.00'), Decimal('22.00'), Decimal('184.00'), 
     "Another comment", "UUID-456", "2024-01-02 10:00:00", "2024-01-02 10:00:00", "input_file_2"),
    (2, 1, "2024-01-02 10:00:00", "2024-01-03 10:00:00", None, 4, True, "SO124", "PO124", "AN124", 2, None, None, 
     15, 25, 35, None, "456DEF", None, Decimal('150.00'), Decimal('12.00'), Decimal('22.00'), Decimal('184.00'), 
     "Another comment", "UUID-456", "2024-01-02 10:00:00", "2024-01-02 10:00:00", "input_file_2"),
    (3, 1, "2024-01-01 10:00:00", "2023-12-31 10:00:00", None, 5, True, "SO125", "PO125", "AN125", 3, None, None, 
     10, 20, 30, None, "789GHI", None, Decimal('300.00'), Decimal('15.00'), Decimal('30.00'), Decimal('345.00'), 
     "Fail: DueDate < OrderDate", "UUID-789", "2024-01-01 10:00:00", "2024-01-01 10:00:00", "input_file_3"),
    (4, 1, "2024-01-01 10:00:00", "2024-01-02 10:00:00", None, 5, True, "SO126", "PO126", "AN126", 4, None, None, 
     10, 20, 30, None, "012JKL", None, Decimal('300.00'), Decimal('15.00'), Decimal('-5.00'), Decimal('345.00'), 
     "Fail: Freight < 0", "UUID-012", "2024-01-01 10:00:00", "2024-01-01 10:00:00", "input_file_4"),
    (5, 1, "2024-01-01 10:00:00", "2024-01-02 10:00:00", "2023-12-31 10:00:00", 5, True, "SO127", "PO127", "AN127", 5, None, None, 
     10, 20, 30, None, "345MNO", None, Decimal('300.00'), Decimal('15.00'), Decimal('30.00'), Decimal('345.00'), 
     "Fail: ShipDate < OrderDate", "UUID-345", "2024-01-01 10:00:00", "2024-01-01 10:00:00", "input_file_5"),
    (6, 1, "2024-01-01 10:00:00", "2024-01-02 10:00:00", None, 9, True, "SO128", "PO128", "AN128", 6, None, None, 
     10, 20, 30, None, "678PQR", None, Decimal('300.00'), Decimal('15.00'), Decimal('30.00'), Decimal('345.00'), 
     "Fail: Status > 8", "UUID-678", "2024-01-01 10:00:00", "2024-01-01 10:00:00", "input_file_6"),
    (7, 1, "2024-01-01 10:00:00", "2024-01-02 10:00:00", None, 5, True, "SO129", "PO129", "AN129", 7, None, None, 
     10, 20, 30, None, "901STU", None, Decimal('-100.00'), Decimal('15.00'), Decimal('30.00'), Decimal('345.00'), 
     "Fail: SubTotal < 0", "UUID-901", "2024-01-01 10:00:00", "2024-01-01 10:00:00", "input_file_7"),
    (8, 1, "2024-01-01 10:00:00", "2024-01-02 10:00:00", None, 5, True, "SO130", "PO130", "AN130", 8, None, None, 
     10, 20, 30, None, "234VWX", None, Decimal('300.00'), Decimal('-15.00'), Decimal('30.00'), Decimal('345.00'), 
     "Fail: TaxAmt < 0", "UUID-234", "2024-01-01 10:00:00", "2024-01-01 10:00:00", "input_file_8")
]
# data = [
#     (1, 1, "2024-01-01 10:00:00", "2024-01-02 10:00:00", "2024-01-03 10:00:00", 5, True, "SO123", "PO123", "AN123", 1, None, None, 10, 20, 30, None, "123ABC", None, 100.00, 10.00, 20.00, 130.00, "Sample comment", "UUID-123", "2024-01-01 10:00:00", "2024-01-01 10:00:00", "input_file_1"),
#     (2, 1, "2024-01-02 10:00:00", "2024-01-03 10:00:00", None, 4, True, "SO124", "PO124", "AN124", 2, None, None, 15, 25, 35, None, "456DEF", None, 150.00, 12.00, 22.00, 184.00, "Another comment", "UUID-456", "2024-01-02 10:00:00", "2024-01-02 10:00:00", "input_file_2"),
#     (2, 1, "2024-01-02 10:00:00", "2024-01-03 10:00:00", None, 4, True, "SO124", "PO124", "AN124", 2, None, None, 15, 25, 35, None, "456DEF", None, 150.00, 12.00, 22.00, 184.00, "Another comment", "UUID-456", "2024-01-02 10:00:00", "2024-01-02 10:00:00", "input_file_2"),
#     (3, 1, "2024-01-01 10:00:00", "2023-12-31 10:00:00", None, 5, True, "SO125", "PO125", "AN125", 3, None, None, 10, 20, 30, None, "789GHI", None, 300.00, 15.00, 30.00, 345.00, "Fail: DueDate < OrderDate", "UUID-789", "2024-01-01 10:00:00", "2024-01-01 10:00:00", "input_file_3"),
#     (4, 1, "2024-01-01 10:00:00", "2024-01-02 10:00:00", None, 5, True, "SO126", "PO126", "AN126", 4, None, None, 10, 20, 30, None, "012JKL", None, 300.00, 15.00, -5.00, 345.00, "Fail: Freight < 0", "UUID-012", "2024-01-01 10:00:00", "2024-01-01 10:00:00", "input_file_4"),
#     (5, 1, "2024-01-01 10:00:00", "2024-01-02 10:00:00", "2023-12-31 10:00:00", 5, True, "SO127", "PO127", "AN127", 5, None, None, 10, 20, 30, None, "345MNO", None, 300.00, 15.00, 30.00, 345.00, "Fail: ShipDate < OrderDate", "UUID-345", "2024-01-01 10:00:00", "2024-01-01 10:00:00", "input_file_5"),
#     (6, 1, "2024-01-01 10:00:00", "2024-01-02 10:00:00", None, 9, True, "SO128", "PO128", "AN128", 6, None, None, 10, 20, 30, None, "678PQR", None, 300.00, 15.00, 30.00, 345.00, "Fail: Status > 8", "UUID-678", "2024-01-01 10:00:00", "2024-01-01 10:00:00", "input_file_6"),
#     (7, 1, "2024-01-01 10:00:00", "2024-01-02 10:00:00", None, 5, True, "SO129", "PO129", "AN129", 7, None, None, 10, 20, 30, None, "901STU", None, -100.00, 15.00, 30.00, 345.00, "Fail: SubTotal < 0", "UUID-901", "2024-01-01 10:00:00", "2024-01-01 10:00:00", "input_file_7"),
#     (8, 1, "2024-01-01 10:00:00", "2024-01-02 10:00:00", None, 5, True, "SO130", "PO130", "AN130", 8, None, None, 10, 20, 30, None, "234VWX", None, 300.00, -15.00, 30.00, 345.00, "Fail: TaxAmt < 0", "UUID-234", "2024-01-01 10:00:00", "2024-01-01 10:00:00", "input_file_8")
# ]

# Criar DataFrame com dados fictícios
df = spark.createDataFrame(data, schema)
df_casted = df.select(
    df["SalesOrderID"].cast(IntegerType()),
    df["RevisionNumber"].cast(IntegerType()),
    df["OrderDate"].cast(TimestampType()),
    df["DueDate"].cast(TimestampType()),
    df["ShipDate"].cast(TimestampType()),
    df["Status"].cast(IntegerType()),
    df["OnlineOrderFlag"].cast(BooleanType()),
    df["SalesOrderNumber"].cast(StringType()),
    df["PurchaseOrderNumber"].cast(StringType()),
    df["AccountNumber"].cast(StringType()),
    df["CustomerID"].cast(IntegerType()),
    df["SalesPersonID"].cast(IntegerType()),
    df["TerritoryID"].cast(IntegerType()),
    df["BillToAddressID"].cast(IntegerType()),
    df["ShipToAddressID"].cast(IntegerType()),
    df["ShipMethodID"].cast(IntegerType()),
    df["CreditCardID"].cast(IntegerType()),
    df["CreditCardApprovalCode"].cast(StringType()),
    df["CurrencyRateID"].cast(IntegerType()),
    df["SubTotal"].cast(DecimalType(19, 4)),
    df["TaxAmt"].cast(DecimalType(19, 4)),
    df["Freight"].cast(DecimalType(19, 4)),
    df["TotalDue"].cast(DecimalType(19, 4)),
    df["Comment"].cast(StringType()),
    df["rowguid"].cast(StringType()),
    df["ModifiedDate"].cast(TimestampType()),
    df["_process_timestamp"].cast(TimestampType()),
    df["_input_file_name"].cast(StringType())
)

# Mostrar o DataFrame resultante
df_casted.printSchema()

tranforming_salesOrderHeader(df_casted)

# COMMAND ----------

tranforming_salesOrderHeader(df_casted,silver_target_table_name,primary_keys,expected_schema)

# df_salesOrderHeader_transformed = tranforming_salesOrderHeader(
#     df_salesOrderHeader=df_salesOrderHeader_bronze,
#     sink_table_name=silver_target_table_name,
#     primary_keys=primary_keys
# )
