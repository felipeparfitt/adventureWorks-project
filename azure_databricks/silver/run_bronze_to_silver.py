# Databricks notebook source
# MAGIC %md
# MAGIC ## Running Bronze to Silver Notebooks in Parallel

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label='Enter the environment in lower case')
env = dbutils.widgets.get('env')

# COMMAND ----------

# MAGIC %run "../utils/common_variables"

# COMMAND ----------

# Function to run a notebook
def run_notebook(notebook_path):
    return dbutils.notebook.run(notebook_path, 300, {"env": env})

with ThreadPoolExecutor() as executor:
    futures = []
    for table_name, table_info in adventureworks_tables_info.items():
        if table_info['active']:
            futures.extend([executor.submit(run_notebook, table_info['notebook_path'])])
results = [future.result() for future in futures]
