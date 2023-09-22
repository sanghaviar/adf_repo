# Databricks notebook source
# DBTITLE 1,SFTP Mount Point
# try:    
#     dbutils.fs.mount(
#         source = 'wasbs://sftp@assignment1storacc.blob.core.windows.net/',
#         mount_point = '/mnt/file3',
#         extra_configs={'fs.azure.account.key.assignment1storacc.blob.core.windows.net':'+5MJ9aigG2dSGpacGhoUvpWqvxUaAMguI+Qc4NZsDXDZgku4aTERWrzFCA2lP/QME4t2pw1VZ9FB+AStulLHqA=='}
#     )
# except:
#     print("Already Mounted") 

# COMMAND ----------

# MAGIC %md
# MAGIC INPUTS   
# MAGIC bronze_folder_name = 07-09-2023   
# MAGIC database_name = silver_database_2     
# MAGIC file_path = Accommodation_1.csv  
# MAGIC sftp_folder_name = working    
# MAGIC source_Path = Accommodation  
# MAGIC primary_key = Entry_Key

# COMMAND ----------

# MAGIC %md
# MAGIC sftp_path = "/mnt/file3/uploads/Accommodation/working/Accommodation_1.csv"  
# MAGIC bronze_path = /mnt/file1/Accommodation/07-09-2023/Accommodation_1.csv   
# MAGIC silver_path = /mnt/file2/silver_database_2/Accommodation

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import col,count

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text('src_path', 'default_value', 'source_Path')
src_path = dbutils.widgets.get('src_path')

dbutils.widgets.text('sftp_foldername', 'default_value', 'sftp_folder_name')
sftp_foldername = dbutils.widgets.get('sftp_foldername')

dbutils.widgets.text('bronze_foldername', 'default_value', 'bronze_folder_name')
bronze_foldername = dbutils.widgets.get('bronze_foldername')

dbutils.widgets.text('file_path', 'default_value', 'file_path')
file_path = dbutils.widgets.get('file_path')

dbutils.widgets.text('database_name','default_value','database_name')
database_name = dbutils.widgets.get('database_name')

dbutils.widgets.text('primary_key','default_value','primary_key')
primary_key = dbutils.widgets.get('primary_key')
# dbutils.widgets.removeAll()

# COMMAND ----------

data_format="csv"
data_format1 = "delta"
options={
    "header":True,
    "delimiter":',',
    "encoding":"UTF-16",
    "multiline":True,
    "inferSchema":True
}
options1 = {}
sftp_path = f"/mnt/file3/uploads/{src_path}/{sftp_foldername}/{file_path}"
bronze_path = f"/mnt/file1/{src_path}/{bronze_foldername}/{file_path}"
silver_path = f"/mnt/file2/{database_name}/{src_path}"

sftp_source_data = spark.read.format(data_format).options(**options).load(sftp_path)
bronze_data = spark.read.format(data_format).options(**options).load(bronze_path)
silver_data = spark.read.format(data_format1).load(silver_path)

# COMMAND ----------

# DBTITLE 1,Comparing Source and Bronze Schemas
def comparing_schemas(sftp_source_data,bronze_data):
    assert bronze_data.schema == sftp_source_data.schema, "Schema mismatch between bronze_data and source_data"
    sftp_source_data.printSchema()
    bronze_data.printSchema()
comparing_schemas(sftp_source_data,bronze_data)

# COMMAND ----------

# DBTITLE 1,Comparing Bronze and Silver Schema
def comparing_schemas1(bronze_data,silver_data):
    bronze_data.printSchema()
    silver_data.printSchema()
    
    try:
        assert bronze_data.schema == silver_data.schema, "Schema mismatch between bronze_data and silver_data"
    except AssertionError as e:
        print(e)
comparing_schemas1(bronze_data,silver_data)

# COMMAND ----------

# DBTITLE 1,Comparing Row Count of Source and Bronze 
def row_count(sftp_source_data,bronze_data):
    source_file = sftp_source_data.count()
    bronze_file = bronze_data.count()
    print("Source Row Count",source_file)
    print("Bronze Row Count",bronze_file)
    assert source_file==bronze_file,"Row Counts of Source and Bronze do not Match"

row_count(sftp_source_data,bronze_data)

# COMMAND ----------

# DBTITLE 1,Comaparing Row Count of Bronze and Silver
def row_count1(bronze_data,silver_data):
    bronze_file = bronze_data.count()
    silver_table = silver_data.count()
    print("Row Count of Bronze",bronze_file)
    print("Row Count of Silver",silver_table)
    try:
        assert bronze_file == silver_table,"Row Count of Bronze and Silver do not Match"
    except AssertionError as e:
        print(e)   
row_count1(bronze_data,silver_data)         

# COMMAND ----------

# DBTITLE 1,Comparing Column Count of Source and Bronze
def column_count(sftp_source_data,bronze_data):  
    source_columns = len(sftp_source_data.columns)
    bronze_columns = len(bronze_data.columns)
    print("Source Column Count",source_columns)
    print("Bronze Column Count",bronze_columns)
    assert source_columns == bronze_columns,"Column Count Does not Match"
column_count(sftp_source_data,bronze_data)    

# COMMAND ----------

# DBTITLE 1,Comaparing Column count of Bronze and Silver
def column_count1(bronze_data,silver_data):
    bronze_columns = len(bronze_data.columns)
    silver_columns = len(silver_data.columns)
    print("Bronze Column Count",bronze_columns)
    print("Silver Column Count",silver_columns)
    try:
        assert bronze_columns == silver_columns,"Column count of Bronze and Silver do not Match"
    except AssertionError as e:
        print(e)    
column_count1(bronze_data,silver_data)        

# COMMAND ----------

# DBTITLE 1,Check for Null Values in Primary Key Column in Silver
#Assuming Entry_Key as primanry key 
def primary_key_null(silver_data):
    silver_file_null_count = silver_data.filter(col(primary_key).isNull()).count()
    assert silver_file_null_count == 0,"Null Values present in Primary Key"
primary_key_null(silver_data)

# COMMAND ----------

# DBTITLE 1,Check for Duplicates in Primary Key in Silver
def silver_duplicates(silver_data):
    duplicate_count_silver = silver_data.groupBy(primary_key).count().filter("count > 1").count()
    assert duplicate_count_silver == 0,"Duplicates found in Primary Key"
silver_duplicates(silver_data)    

# COMMAND ----------



# COMMAND ----------


