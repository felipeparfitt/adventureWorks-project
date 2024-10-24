# Databricks notebook source
# MAGIC %md
# MAGIC # ETL: Landing Zone to Bronze Layer
# MAGIC This notebook provides functions to handle data ingestion and storage. These functions read files from the landing zone and load them as Delta tables in the bronze layer.
# MAGIC
# MAGIC ## Batch Mode
# MAGIC - **Reading Data in Batch Mode:** Reads parquet data from a landing zone and adds two columns: _process_timestamp and _input_file_name. It returns the DataFrame for further processing.
# MAGIC
# MAGIC - **Writing Data in Batch Mode:** Writes the DataFrame to a Delta table in the bronze layer, overwriting any existing data. It efficiently handles batch data storage.

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label='Enter the environment in lower case')
env = dbutils.widgets.get('env')

# COMMAND ----------

# MAGIC %run "../utils/common_variables"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Useful Functions (Batch Mode):

# COMMAND ----------

# Reading data in batch mode:
def read_landing_data_batch(
        landing_path: str, 
        schema_name: str, 
        table_name: str
    ) -> DataFrame:
    """
    Reads data from the landing zone in batch mode.

    Parameters:
        landing_path (str): The base path of the landing zone.
        schema_name (str): The schema name of the table.
        table_name (str): The name of the table to read.

    Returns:
        DataFrame: A Spark DataFrame containing the data read from the landing zone.
    """
    print(f"(BATCH) Reading the landing {table_name} table from landing zone: ", end='')
    landing_df = (
        spark.read
             .format('parquet')
             .load(f'{landing_path}/{schema_name}/{table_name}')
             .withColumn('_process_timestamp', F.current_timestamp())
             .withColumn('_input_file_name', F.input_file_name())
    )
    print("Success !!")
    return landing_df
    
# Writing data in batch mode:
def write_bronze_data_batch(
        df: DataFrame, 
        bronze_path: str, 
        schema_name: str, 
        table_name: str,
        primary_keys: dict,
    ) -> None:
    """
    Writes data to the bronze layer in batch mode.

    Parameters:
        df (DataFrame): The Spark DataFrame to write.
        bronze_path (str): The base path of the bronze layer.
        schema_name (str): The schema name of the table.
        table_name (str): The name of the table to write.

    Returns:
        None
    """
    print(f"(BATCH) Write {table_name} to bronze layer: ", end='')
    
    # Verify if the table exists in the bronze layer
    if DeltaTable.isDeltaTable(spark, f"{bronze_path}/{schema_name}/{table_name}"):

        # Getting the primary keys of the table
        comparative_keys = [f"target.{primary_key} = source.{primary_key}" for primary_key in primary_keys]
        comparative_keys = " AND ".join(comparative_keys) if len(primary_keys) > 1 else comparative_keys[0]

        # Reading the bronze table if it exists
        bronze_delta_table = DeltaTable.forPath(spark, f"{bronze_path}/{schema_name}/{table_name}")

        bronze_delta_table.alias('target').merge(
            source=df.alias('source'),
            condition=comparative_keys
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .whenNotMatchedBySourceDelete() \
         .execute()
    else:
        (df.write
            .format('delta')
            .option('mergeSchema', 'true') # enables schema evolution in bronze layer
            .mode('overwrite')
            .save(f"{bronze_path}/{schema_name}/{table_name}")
    )
    print("Success !!")
    print("*******************************")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Reading/Writing all tables to bronze layer:

# COMMAND ----------

# Batch Mode
for table_name, table_info in adventureworks_tables_info.items():
    if table_info['active']:
        # Reading the table from landing zone
        df_batch = read_landing_data_batch(
            landing_path=landing_path,
            schema_name=table_info['schema_name'], 
            table_name=table_name
        )

        # Writing the table to bronze layer
        write_bronze_data_batch(
            df=df_batch, 
            bronze_path=bronze_path, 
            schema_name=table_info['schema_name'], 
            table_name=table_name,
            primary_keys=table_info['primary_keys'],
        )

        # Creating the table in unity catalog
        spark.sql(f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {catalog_name}.bronze.{table_info['schema_name']}_{table_name}
            USING DELTA 
            LOCATION '{bronze_path}/{table_info["schema_name"]}/{table_name}'
            """
        )
