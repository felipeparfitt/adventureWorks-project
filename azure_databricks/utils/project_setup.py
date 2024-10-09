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
# MAGIC
# MAGIC - **Create Tables in Schemas:**
# MAGIC Handles the creation of tables within a specified schema and catalog by executing predefined SQL commands, ensuring proper placement and management of the tables.

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

# def creating_tables(environment, layer, table_dict):
#     catalog_name = f"dbproj_{environment}"
#     print(f"Using {catalog_name} catalog")
#     spark.sql(f"USE CATALOG {catalog_name};")

#     print(f"Using {layer} schema")
#     spark.sql(f"USE SCHEMA {layer};")

#     for table_name, create_table_sql in table_dict.items():
#         print(f"Creating {table_name} table in {catalog_name}.{layer}: ", end='')
#         spark.sql(create_table_sql)
#         print("Success !!")
#         print("************************************")


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Creating Catalog, Schemas and Tables

# COMMAND ----------

# Dict used to create the tables on bronze layer
sql_tables = {
    "clients": """
        CREATE TABLE IF NOT EXISTS clients (
            client_id INT,
            firstname VARCHAR(30),
            lastname VARCHAR(30),
            birth_date DATE,
            email STRING,
            phone CHAR(20),
            extract_Time TIMESTAMP
        );
    """,
    "client_addresses": """
        CREATE TABLE IF NOT EXISTS client_addresses (
            address_id INT,
            client_id INT,
            state CHAR(2),
            city VARCHAR(100),
            street VARCHAR(150),
            zip_code VARCHAR(20),
            extract_Time TIMESTAMP
        );
    """,
    "sales_people": """
        CREATE TABLE IF NOT EXISTS sales_people (
            salesperson_id INT,
            firstname VARCHAR(30),
            lastname VARCHAR(30),
            email VARCHAR(100),
            phone_number VARCHAR(20),
            extract_Time TIMESTAMP
        );
    """,
    "sales": """
        CREATE TABLE IF NOT EXISTS sales (
            sale_id INT,
            client_id INT,
            salesperson_id INT,
            sale_date DATE,
            total_amount DECIMAL(10, 2),
            extract_Time TIMESTAMP
        );
    """,
    "sales_items": """
        CREATE TABLE IF NOT EXISTS sales_items (
            item_id INT,
            sale_id INT,
            product_id INT,
            quantity INT,
            unit_price DECIMAL(10, 2),
            total_amount DECIMAL(10, 2),
            discount DECIMAL(10, 2),
            extract_Time TIMESTAMP
        );
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS products (
            product_id INT,
            product_name VARCHAR(150),
            description STRING,
            price DECIMAL(10, 2),
            extract_Time TIMESTAMP
        );
    """
}

# COMMAND ----------

# Creating the catalog
create_catalog(catalog_name=catalog_name)

# Creating the schemas in a managed location
create_schema(catalog_name=catalog_name, layer='bronze', path=bronze_path)
create_schema(catalog_name=catalog_name, layer='silver', path=silver_path)
create_schema(catalog_name=catalog_name, layer='gold', path=gold_path)

# Creating all tables needed
#creating_tables(env, 'bronze', sql_tables)
