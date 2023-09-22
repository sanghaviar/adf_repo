# Databricks notebook source
# MAGIC %run /Users/sanghavi.a.r@diggibyte.com/util1

# COMMAND ----------

# /mnt/file1/Accommodation/07-09-2023/Accommodation_1.csv

# COMMAND ----------

# DBTITLE 1,Mount Point for Bronze
# try:    
#     dbutils.fs.mount(
#         source = 'wasbs://bronze@assignment1storacc.blob.core.windows.net/',
#         mount_point = '/mnt/file1',
#         extra_configs={'fs.azure.account.key.assignment1storacc.blob.core.windows.net':'+5MJ9aigG2dSGpacGhoUvpWqvxUaAMguI+Qc4NZsDXDZgku4aTERWrzFCA2lP/QME4t2pw1VZ9FB+AStulLHqA=='}
#     )
# except:
#     print("Already Mounted") 

# COMMAND ----------

# DBTITLE 1,Mount Point for Silver
# try:    
#     dbutils.fs.mount(
#         source = 'wasbs://silver@assignment1storacc.blob.core.windows.net/',
#         mount_point = '/mnt/file2',
#         extra_configs={'fs.azure.account.key.assignment1storacc.blob.core.windows.net':'+5MJ9aigG2dSGpacGhoUvpWqvxUaAMguI+Qc4NZsDXDZgku4aTERWrzFCA2lP/QME4t2pw1VZ9FB+AStulLHqA=='}
#     )
# except:
#     print("Already Mounted")

# COMMAND ----------

# DBTITLE 1,Widgets

dbutils.widgets.text('src_path', 'default_value', 'source_Path')
src_path = dbutils.widgets.get('src_path')

dbutils.widgets.text('foldername', 'default_value', 'folder_name')
foldername = dbutils.widgets.get('foldername')

dbutils.widgets.text('file_path', 'default_value', 'file_path')
file_path = dbutils.widgets.get('file_path')

# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Reading CSV Data from Bronze
data_format="csv"

# path = /mnt/file1/Accommodation/07-09-2023/Accommodation_1.csv
options={
    "header":True,
    "delimiter":',',
    "encoding":"UTF-16",
    "multiline":True,
    "inferSchema":True
}
bronze_path = ("/mnt/file1/"+src_path+"/"+foldername+"/"+file_path)
csv_df=read_csv(data_format,options,bronze_path)
display(csv_df)

# COMMAND ----------

# special_char = regular_exp(replace_null_values)
# display(special_char)

# COMMAND ----------

# DBTITLE 1,Replacing Columns with UnderScore
col_spaces = rep_columns(csv_df)#.withColumnRenamed("Average_Room_Rate_(SEK)","Average_Room_Rate_SEK")
display(col_spaces)



# COMMAND ----------

unique_combinations_count = col_spaces.select('Entry_Key').distinct().count()
print(unique_combinations_count)


# COMMAND ----------

# DBTITLE 1,Adding Load Date
adding_date_column = adding_load_date(col_spaces)
display(adding_date_column)

# COMMAND ----------

# DBTITLE 1,Checking for Null Values
null_val = null_check(csv_df)
display(null_val)

# COMMAND ----------

# DBTITLE 1,Replacing Null Values
replace_null_values = replace_null(adding_date_column,file_path)
display(replace_null_values)

# COMMAND ----------

# DBTITLE 1,Handling Special charecter
special_char = regular_exp(replace_null_values)
display(special_char)

# COMMAND ----------

# DBTITLE 1,Writting to Silver Layer
database_name = "silver_database_2"
delta_path =  f"/mnt/file2/{database_name}/{src_path}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")
special_char.write.mode("overwrite").format("delta").option("path",delta_path).saveAsTable(f"{database_name}.{src_path}")


# COMMAND ----------

# MAGIC %sql 
# MAGIC show views;

# COMMAND ----------

# special_char.createOrReplaceTempView(src_path)
# spark.sql("""MERGE INTO database_name.src_path AS TARGET USING src_path AS SOURCE
#           ON TARGET.COLUMN = SOURCE.COLUMN
#           WHEN MATCHED 
#           THEN UPDATE SET 
#           TARGET.COLUMN_UPDATE1 = SOURCE.COLUMN_UPDATE1, 
#           TARGET.COLUMN_UPDATE2 = SOURCE.COLUMN_UPDATE2
#           WHEN NOT MATCHED THEN INSERT(
#           TARGET.COLUMN1,TARGET.COLUMN2)
#           VALUES (
#               SOURCE.COLUMN1,SOURCE.COLUMN2
#           )""")

# COMMAND ----------

# merge_sql = f"""
#     MERGE INTO {database_name}.{src_path} AS TARGET
#     USING {src_path} AS SOURCE
#     ON {", ".join([f'TARGET.{col} = SOURCE.{col}' for col in column_names])}
#     WHEN MATCHED THEN UPDATE SET {update_clause}
#     WHEN NOT MATCHED THEN INSERT ({", ".join(column_names)}) VALUES ({", ".join([f'SOURCE.{col}' for col in column_names])})
# """

# COMMAND ----------

# column_names = special_char.columns

# # Create a dynamic list of update expressions for the `SET` clause
# update_expressions = [f"TARGET.{col} = SOURCE.{col}" for col in column_names]

# # Join the update expressions into a comma-separated string
# update_clause = ", ".join(update_expressions)

# Generate the dynamic `MERGE INTO` statement
merge_sql = f"""
    MERGE INTO {database_name}.{src_path} AS TARGET
    USING {src_path} AS SOURCE
    ON {", ".join([f'TARGET.{col} = SOURCE.{col}' for col in column_names])}
    WHEN MATCHED THEN UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN INSERT ({", ".join(column_names)}) VALUES ({", ".join([f'SOURCE.{col}' for col in column_names])})
"""


# COMMAND ----------

# %sql 
# show databases;

# COMMAND ----------

# dbutils.fs.rm("/mnt/file2/silver_database/", True)

# COMMAND ----------

# %sql 
# drop database silver_database cascade;

# COMMAND ----------

# # def csv_silver(colupper_df_csv):
# silver_csv = csv_df.write \
#         .format("delta") \
#         .mode("overwrite")\
#         .saveAsTable('/mnt/file2/silver/silver_table')


# COMMAND ----------

# # silver_path = "/mnt/file2/Accomodation/07-09-2023"
# col_spaces.write.format("delta").option("overWriteSchema","True").mode('append').partitionBy('column').save('delta_path')


# COMMAND ----------

# spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
# data = col_spaces.write.mode("overwrite").format("delta").option("path",delta_path).saveAsTable(f"{database_name}.{table_name}")

# COMMAND ----------

# spark.sql("CREATE TABLE IF NOT EXISTS silver_database.silver_table USING DELTA LOCATION")
