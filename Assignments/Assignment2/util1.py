# Databricks notebook source
from pyspark.sql.types import * 
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Reading CSV Data
def read_csv(data_format,options,bronze_path):
    return spark.read.format(data_format).options(**options).load(bronze_path)

# COMMAND ----------

# DBTITLE 1,Checking for nulls
def null_check(df):
    exprs = []
    for c in df.columns:
        exprs.append(count(when(col(c).isNull(), c)).alias(c))
    return df.select(*exprs)

# COMMAND ----------

# DBTITLE 1,Adding Date Column
def adding_load_date(df):
    return df.withColumn("load_date",date_format(lit(current_date()),"MM-dd-yyyy"))

# COMMAND ----------

# DBTITLE 1,Replacing Null Values
def replace_null(df,file_path):
    if file_path in ["entertainment", "expenses"]:
        for c in df.columns:
             df = df.withColumn(c, when(col(c).isNull(), 0).otherwise(col(c)))
        return df
    else:
        return df

# COMMAND ----------

# DBTITLE 1,Handling  Special Charecters
def regular_exp(df):
   original_data_types = df.dtypes
   for col in df.columns:
        df = df.withColumn(col, regexp_replace(col, "[^a-zA-Z0-9 ]",""))
   for col_name, data_type in original_data_types:
        df = df.withColumn(col_name, df[col_name].cast(data_type))    
   return df

# COMMAND ----------

# DBTITLE 1,Modifying Column names by adding underscore 
def rep_columns(df):
    column_list = [i.replace(" ","_")for i in df.columns]
    column_list = [col.replace("(", "").replace(")", "").replace("_", " ").replace(" ", "_") for col in df.columns]
    df = df.toDF(*column_list) # Here toDF is used to create a dataFrame from RDD
    return df
