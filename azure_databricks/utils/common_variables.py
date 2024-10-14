# Databricks notebook source
# MAGIC %md
# MAGIC # Common Variables

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, ByteType, BooleanType, 
    TimestampType, DecimalType, ShortType
)
from delta.tables import DeltaTable

# COMMAND ----------

# Setting up the environment
dbutils.widgets.text(name='env', defaultValue="", label='Enter the environment in lower case')
env = dbutils.widgets.get('env')

# COMMAND ----------

# Project Name
project_name = "adventureworks"
# Catalog Name
catalog_name = f"{project_name}_{env}"
# Getting the External Locations path
landing_path = spark.sql(f"DESCRIBE EXTERNAL LOCATION {project_name}_landing_{env}").select('url').collect()[0][0]
bronze_path = spark.sql(f"DESCRIBE EXTERNAL LOCATION {project_name}_bronze_{env}").select('url').collect()[0][0]
silver_path = spark.sql(f"DESCRIBE EXTERNAL LOCATION {project_name}_silver_{env}").select('url').collect()[0][0]
gold_path = spark.sql(f"DESCRIBE EXTERNAL LOCATION {project_name}_gold_{env}").select('url').collect()[0][0]

# COMMAND ----------

# Tables info with primary keys
adventureworks_tables_info = {
    "SalesOrderHeader": {
        "active": True,
        "schema_name": "Sales",
        "notebook_path": "/path/to/notebook/SalesOrderHeader",
        "primary_keys": ["SalesOrderID"]
    },
    "SalesOrderDetail": {
        "active": True,
        "schema_name": "Sales",
        "notebook_path": "/path/to/notebook/SalesOrderDetail",
        "primary_keys": ["SalesOrderID", "SalesOrderDetailID"]
    },
    "Customer": {
        "active": True,
        "schema_name": "Sales",
        "notebook_path": "/path/to/notebook/Customer",
        "primary_keys": ["CustomerID"]
    },
    "SpecialOffer": {
        "active": True,
        "schema_name": "Sales",
        "notebook_path": "/path/to/notebook/SpecialOffer",
        "primary_keys": ["SpecialOfferID"]
    },
    "Currency": {
        "active": True,
        "schema_name": "Sales",
        "notebook_path": "/path/to/notebook/Currency",
        "primary_keys": ["CurrencyCode"]
    },
    "SalesTerritory": {
        "active": True,
        "schema_name": "Sales",
        "notebook_path": "/path/to/notebook/SalesTerritory",
        "primary_keys": ["TerritoryID"]
    },
    "SalesReason": {
        "active": True,
        "schema_name": "Sales",
        "notebook_path": "/path/to/notebook/SalesReason",
        "primary_keys": ["SalesReasonID"]
    },
    "Address": {
        "active": True,
        "schema_name": "Person",
        "notebook_path": "/path/to/notebook/Address",
        "primary_keys": ["AddressID"]
    },
    "EmailAddress": {
        "active": True,
        "schema_name": "Person",
        "notebook_path": "/path/to/notebook/EmailAddress",
        "primary_keys": ["EmailAddressID"]
    },
    "PersonPhone": {
        "active": True,
        "schema_name": "Person",
        "notebook_path": "/path/to/notebook/PersonPhone",
        "primary_keys": ["BusinessEntityID", "PhoneNumber", "PhoneNumberTypeID"]
    },
    "StateProvince": {
        "active": True,
        "schema_name": "Person",
        "notebook_path": "/path/to/notebook/StateProvince",
        "primary_keys": ["StateProvinceID"]
    },
    "CountryRegion": {
        "active": True,
        "schema_name": "Person",
        "notebook_path": "/path/to/notebook/CountryRegion",
        "primary_keys": ["CountryRegionCode"]
    },
    "ProductCostHistory": {
        "active": True,
        "schema_name": "Production",
        "notebook_path": "/path/to/notebook/ProductCostHistory",
        "primary_keys": ["ProductID", "StartDate"]
    },
    "Product": {
        "active": True,
        "schema_name": "Production",
        "notebook_path": "/path/to/notebook/Product",
        "primary_keys": ["ProductID"]
    },
    "ProductSubcategory": {
        "active": True,
        "schema_name": "Production",
        "notebook_path": "/path/to/notebook/ProductSubcategory",
        "primary_keys": ["ProductSubcategoryID"]
    },
    "ProductModel": {
        "active": True,
        "schema_name": "Production",
        "notebook_path": "/path/to/notebook/ProductModel",
        "primary_keys": ["ProductModelID"]
    },
    "ProductDescription": {
        "active": True,
        "schema_name": "Production",
        "notebook_path": "/path/to/notebook/ProductDescription",
        "primary_keys": ["ProductDescriptionID"]
    }
}


# COMMAND ----------

# MAGIC %md
# MAGIC ## USEFUL FUNCTIONS:

# COMMAND ----------

def add_constraints(
    table_name: str, 
    constraints: list
    ) -> None:
    """
    Adds constraints to a specified table in Spark SQL.

    Parameters:
        table_name (str): The name of the table to which constraints will be added.
        constraints (list): A list of SQL constraint statements to be applied to the table.

    Raises:
        Exception: Raises an exception for any error other than "already exists" when adding constraints.
    """
    for constraint in constraints:
        try:
            spark.sql(f"ALTER TABLE {table_name} {constraint}")
            print(f"Constraint {constraint} added successfully.")
        except Exception as e:
            if "already exists" in str(e):
                print(f"Constraint {constraint} already exists, skipping addition.")
            else:
                raise


def get_table_constraints_conditions(table_name: str) -> str:
    """
    Retrieves the table constraints from the Delta table properties and 
    generates a combined condition string to apply them.

    Parameters:
        table_name (str): The name of the Delta table to retrieve constraints from.

    Returns:
        str: A string combining all constraints with 'AND' to apply them as conditions.
    """
    # Query the table properties to retrieve constraints starting with "delta.cons"
    constraints_df = (
        spark.sql(f"SHOW TBLPROPERTIES {table_name}")
        .filter(F.col("key").startswith("delta.cons"))
        .select("value")
    )

    # Collect all constraints into a list of strings
    constraints_list = [row["value"] for row in constraints_df.collect()]

    # If no constraints are found, return a condition that always evaluates to true
    if not constraints_list:
        return "1=1"

    # Combine the constraints into a single condition string with "AND"
    combined_conditions = " AND ".join([f"({condition})" for condition in constraints_list])
    
    return combined_conditions

def df_deduplicate(
    df: DataFrame, 
    primary_keys: list, 
    order_col: str
) -> DataFrame:
    """
    Deduplicates a DataFrame based on primary key columns and an order column.

    Parameters:
        df (DataFrame): The input DataFrame to be deduplicated.
        primary_keys (list): List of columns that define the primary key for deduplication. 
                             These columns are used to identify duplicates.
        order_col (str): The column to use for ordering within each partition to keep the latest record.

    Returns:
        DataFrame: A deduplicated DataFrame with only one record per primary key, 
                   keeping the record with the highest value in the order column.
    """
    # Define a window function that partitions by primary keys and orders by the order column
    window_func = Window.partitionBy(*primary_keys).orderBy(F.col(order_col).desc())

    # Apply row_number() to assign ranks and filter to keep only the top-ranked record in each partition
    df_dedup = (
        df.withColumn('rank', F.row_number().over(window_func))  # Rank records within partitions
          .filter(F.col('rank') == 1)  # Keep only the first-ranked record (latest by order_col)
          .drop('rank')  # Drop the rank column after filtering
    )

    return df_dedup


def verify_schema(
    df: DataFrame,
    expected_schema: StructType
) -> None:
    """
    Verifies the schema of a given DataFrame against an expected schema.

    Parameters:
        df (DataFrame): The Spark DataFrame whose schema needs to be verified.
        expected_schema (StructType): The expected schema as a StructType object.

    Returns:
        None: Raises a ValueError if there are discrepancies between the actual and expected schema.
    """
    discrepancies = []  # List to store discrepancies

    # Iterate over each field in the DataFrame schema
    for field in df.schema:
        col_name = field.name
        actual_data_type = field.dataType
        
        # Find the matching column in the expected schema
        expected_field = next((f for f in expected_schema if f.name == col_name), None)
        
        if expected_field is None:
            discrepancies.append(f"Column '{col_name}' is not present in the expected schema.")
            continue

        expected_data_type = expected_field.dataType
        
        # Check if the column type is different
        if actual_data_type != expected_data_type:
            discrepancies.append(f"Column '{col_name}' has actual type {actual_data_type}, but should be {expected_data_type}")
    
    # If there are discrepancies, raise an error after the complete check
    if discrepancies:
        print("Discrepancies found:")
        for discrepancy in discrepancies:
            print(discrepancy)
        
        raise ValueError("The DataFrame schema has discrepancies. Check the listed columns.")
    else:
        print("The schema is correct.")

def upsert_delta_table(
      df_source_table: DataFrame, 
      sink_table_name: str, 
      primary_keys: list
    ) -> None:
    """
    Upserts data from a source DataFrame into a Delta table.

    Parameters:
        df_source_table (DataFrame): The DataFrame containing the source data to be upserted.
        sink_table_name (str): The name of the Delta table where data will be upserted.
        primary_keys (list): A list of column names that serve as primary keys for matching records.

    Raises:
        Exception: Raises an exception if the Delta table does not exist.
    """
    print(f"Upserting the {sink_table_name}: ", end='')

    # Verify if the table 
    #DeltaTable.isDeltaTable(spark, sink_table_name): ## nao funciona 
    if spark.catalog.tableExists(sink_table_name):

        # Getting the primary keys of the table
        comparative_keys = [f"target.{primary_key} = source.{primary_key}" for primary_key in primary_keys]
        comparative_keys = " AND ".join(comparative_keys) if len(primary_keys) > 1 else comparative_keys[0]

        # Reading the sink table
        sink_delta_table = DeltaTable.forName(spark, sink_table_name)

        sink_delta_table.alias('target').merge(
            source=df_source_table.alias('source'),
            condition=comparative_keys
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .whenNotMatchedBySourceDelete() \
         .execute()
    else:
        raise Exception(f"Delta table: {sink_table_name} not found!")
    
    print("Success !!")
    print("*******************************")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
