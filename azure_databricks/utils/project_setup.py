# Databricks notebook source
# MAGIC %md
# MAGIC # Project Setup Notebook
# MAGIC This notebook provides functions to automate the setup of data catalogs, schemas, and tables.
# MAGIC
# MAGIC - **Create All Catalogs:**
# MAGIC Automates the process of setting up data catalogs for different environments (like dev, test, or prod), ensuring each environment has its own dedicated catalog.
# MAGIC
# MAGIC - **Create Schemas with Managed Storage:**
# MAGIC Sets up schemas within a catalog, organizing data into different layers (such as bronze, silver, or gold) and assigning a specific storage location for each schema.

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label='Enter the environment in lower case')
env = dbutils.widgets.get('env')

# COMMAND ----------

# MAGIC %run "./common_variables"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Useful Functions:

# COMMAND ----------

def create_catalog(catalog_name):
    print(f"Creating the {catalog_name} Catalog")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name};")
    print("************************************")

def create_schema(catalog_name, layer, path):
    print(f'Creating {layer} schema in {catalog_name}')
    spark.sql(f"""
              CREATE SCHEMA IF NOT EXISTS {catalog_name}.{layer}
              MANAGED LOCATION '{path}';
              """)
    print("************************************")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Creating Catalog and all schemas

# COMMAND ----------

# Creating the catalog
create_catalog(catalog_name=catalog_name)

# Creating the schemas in a managed location
create_schema(catalog_name=catalog_name, layer='bronze', path=bronze_path)
create_schema(catalog_name=catalog_name, layer='silver', path=silver_path)
create_schema(catalog_name=catalog_name, layer='gold', path=gold_path)
