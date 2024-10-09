# Databricks notebook source
# MAGIC %md
# MAGIC # Common Variables

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

