# Databricks notebook source
# MAGIC %md
# MAGIC ## Running Silver to Gold Notebooks in Parallel

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label='Enter the environment in lower case')
env = dbutils.widgets.get('env')

# COMMAND ----------

# MAGIC %run "../utils/common_variables"

# COMMAND ----------

# Dictionary contaning only dimension tables
dw_adventureworks_dim_tables_info = {
    table_name: table_info
    for table_name, table_info in dw_adventureworks_tables_info.items()
    if 'Dim' in table_name
}

# COMMAND ----------

# Function to run a notebook
def run_notebook(notebook_path):
    return dbutils.notebook.run(notebook_path, 300, {"env": env})

# Running All Dimension Tables Notebooks in Parallel
with ThreadPoolExecutor() as executor:
    futures = []
    for table_name, table_info in dw_adventureworks_dim_tables_info.items():
        if table_info['active']:
            futures.extend([executor.submit(run_notebook, table_info['notebook_path'])])
results = [future.result() for future in futures]



# COMMAND ----------

# Running Gold_FactInternetSales Notebook
dict_FactInternetSales = dw_adventureworks_tables_info['FactInternetSales']
if dict_FactInternetSales['active']:
    run_notebook(dict_FactInternetSales['notebook_path'])

# COMMAND ----------

# Running Gold_FactInternetSalesReason Notebook
dict_FactInternetSalesReason = dw_adventureworks_tables_info['FactInternetSalesReason']
if dict_FactInternetSalesReason['active']:
    run_notebook(dict_FactInternetSalesReason['notebook_path'])
