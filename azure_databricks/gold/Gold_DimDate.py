# Databricks notebook source
# MAGIC %md
# MAGIC # Creation of the Dimension Date in the Gold Layer
# MAGIC
# MAGIC This notebook creates a base table for the dimension date in the gold layer, containing essential date-related information for use in further analysis.
# MAGIC
# MAGIC ## Process overview
# MAGIC - **Creating the DataFrame:** The DataFrame is generated with key date attributes, such as day names in different languages, day numbers, month names, and fiscal year information, based on a sequence of dates from 2005 to 2014.
# MAGIC
# MAGIC - **Writing to Gold Layer:** The processed DataFrame is written to a Delta table in the gold layer, ensuring the data is available for analysis in a structured format.

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue="", label='Enter the environment in lower case')
env = dbutils.widgets.get('env')

# COMMAND ----------

# MAGIC %run "../utils/common_variables"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Info

# COMMAND ----------

# Table info
table_name = "DimDate"
gold_target_path = f"{gold_path}/{table_name}"
gold_target_table_name = f"{catalog_name}.gold.{table_name}"

# COMMAND ----------

# Dimension date schema
schema = StructType([
    StructField("DateKey", IntegerType(), False), 
    StructField("FullDateAlternateKey", DateType(), False),
    StructField("DayNumberOfWeek", IntegerType(), False),
    StructField("EnglishDayNameOfWeek", StringType(), False),
    StructField("SpanishDayNameOfWeek", StringType(), False),
    StructField("FrenchDayNameOfWeek", StringType(), False),
    StructField("DayNumberOfMonth", IntegerType(), False),
    StructField("DayNumberOfYear", IntegerType(), False),
    StructField("WeekNumberOfYear", IntegerType(), False),
    StructField("EnglishMonthName", StringType(), False),
    StructField("SpanishMonthName", StringType(), False),
    StructField("FrenchMonthName", StringType(), False),
    StructField("MonthNumberOfYear", IntegerType(), False),
    StructField("CalendarQuarter", IntegerType(), False),
    StructField("CalendarYear", IntegerType(), False),
    StructField("CalendarSemester", IntegerType(), False),
    StructField("FiscalQuarter", IntegerType(), False),
    StructField("FiscalYear", IntegerType(), False),
    StructField("FiscalSemester", IntegerType(), False)
])

# COMMAND ----------

# Dictionary mapping English day names to their Spanish and French equivalents
day_name_dict = {
    "Monday": {"Spanish": "Lunes", "French": "Lundi"},
    "Tuesday": {"Spanish": "Martes", "French": "Mardi"},
    "Wednesday": {"Spanish": "Miércoles", "French": "Mercredi"},
    "Thursday": {"Spanish": "Jueves", "French": "Jeudi"},
    "Friday": {"Spanish": "Viernes", "French": "Vendredi"},
    "Saturday": {"Spanish": "Sábado", "French": "Samedi"},
    "Sunday": {"Spanish": "Domingo", "French": "Dimanche"}
}

# Dictionary mapping English month names to their Spanish and French equivalents
month_name_dict = {
    "January": {"Spanish": "Enero", "French": "Janvier"},
    "February": {"Spanish": "Febrero", "French": "Février"},
    "March": {"Spanish": "Marzo", "French": "Mars"},
    "April": {"Spanish": "Abril", "French": "Avril"},
    "May": {"Spanish": "Mayo", "French": "Mai"},
    "June": {"Spanish": "Junio", "French": "Juin"},
    "July": {"Spanish": "Julio", "French": "Juillet"},
    "August": {"Spanish": "Agosto", "French": "Août"},
    "September": {"Spanish": "Septiembre", "French": "Septembre"},
    "October": {"Spanish": "Octubre", "French": "Octobre"},
    "November": {"Spanish": "Noviembre", "French": "Novembre"},
    "December": {"Spanish": "Diciembre", "French": "Décembre"}
}

# Function to get the corresponding day name in the specified language (Spanish or French)
def get_language_day(english_day, language):
    return day_name_dict.get(english_day, {}).get(language, "")

# Function to get the corresponding month name in the specified language (Spanish or French)
def get_language_month(english_month, language):
    return month_name_dict.get(english_month, {}).get(language, "")

# User-defined function (UDF) to use in Spark for translating day names
udf_get_language_day = F.udf(get_language_day, StringType())

# User-defined function (UDF) to use in Spark for translating month names
udf_get_language_month = F.udf(get_language_month, StringType())

# COMMAND ----------

# Create a base DataFrame containing a sequence of dates from 2005-01-01 to 2014-12-31
df_DimDate = spark.range(1).select(
    F.explode(
        F.sequence(
            F.to_date(F.lit('2005-01-01')),
            F.to_date(F.lit('2014-12-31'))
        )
    ).alias('FullDateAlternateKey')  # Column containing the generated dates
)

# Adding new columns based on the date information
df_DimDate = (
    df_DimDate
        .withColumn('DateKey', F.date_format(F.col('FullDateAlternateKey'), 'yyyyMMdd').cast(IntegerType()))
        .withColumn('DayNumberOfWeek', F.dayofweek('FullDateAlternateKey').cast(IntegerType()))
        .withColumn('EnglishDayNameOfWeek', F.date_format('FullDateAlternateKey', 'EEEE').cast(StringType()))
        .withColumn('SpanishDayNameOfWeek', udf_get_language_day(F.col('EnglishDayNameOfWeek'), F.lit("Spanish")))
        .withColumn('FrenchDayNameOfWeek', udf_get_language_day(F.col('EnglishDayNameOfWeek'), F.lit("French")))
        .withColumn('DayNumberOfMonth', F.dayofmonth('FullDateAlternateKey').cast(IntegerType()))
        .withColumn('DayNumberOfYear', F.dayofyear('FullDateAlternateKey').cast(IntegerType()))
        .withColumn('WeekNumberOfYear', F.weekofyear('FullDateAlternateKey').cast(IntegerType()))
        .withColumn('EnglishMonthName', F.date_format('FullDateAlternateKey', 'LLLL').cast(StringType()))
        .withColumn('SpanishMonthName', udf_get_language_month(F.col('EnglishMonthName'), F.lit("Spanish")))
        .withColumn('FrenchMonthName', udf_get_language_month(F.col('EnglishMonthName'), F.lit("French")))
        .withColumn('MonthNumberOfYear', F.month('FullDateAlternateKey').cast(IntegerType()))
        .withColumn('CalendarQuarter', F.quarter('FullDateAlternateKey').cast(IntegerType()))
        .withColumn('CalendarYear', F.year('FullDateAlternateKey').cast(IntegerType()))
        .withColumn('CalendarSemester', F.when(F.quarter('FullDateAlternateKey') <= 2, 1).otherwise(2))
        .withColumn('FiscalQuarter', ((F.col("CalendarQuarter") + 2 - 1) % 4 + 1))
        .withColumn('FiscalYear', F.when(F.col("FiscalQuarter") == 1, F.col("CalendarYear") + 1).otherwise(F.col("CalendarYear")))
        .withColumn('FiscalSemester', F.when(F.col('FiscalQuarter') <= 2, 1).otherwise(2))
)

# Reordering the columns: moving the second column to the first position
columns = df_DimDate.columns
reordered_columns = [columns[1]] + [columns[0]] + columns[2:]
df_DimDate = df_DimDate.select(reordered_columns)

# Apply the correct schema to the DataFrame
df_DimDate = spark.createDataFrame(df_DimDate.rdd, schema)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Writing the gold table

# COMMAND ----------

# Writing data to gold layer
df_DimDate.write.format('delta').mode('overwrite').save(gold_target_path)

# Creating gold external table
spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {gold_target_table_name}
    USING DELTA
    LOCATION '{gold_target_path}'
""")
