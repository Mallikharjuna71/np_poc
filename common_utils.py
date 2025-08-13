# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import *

# COMMAND ----------

"""
    List all files in a given Databricks volume path (non-recursive).

    Args:
        base_path (str): Path to the base directory inside a Databricks volume
                         (e.g., '/Volumes/catalog/schema/volume_name/path').

    Returns:
        list: A list of file paths (strings) located directly in the given base path.
              Directories are excluded. If an error occurs, an empty list is returned.

    Notes:
        - This function does NOT search recursively in subdirectories.
        - Works with Databricks volumes and any DBFS path accessible via dbutils.fs.ls.
"""
def list_files_in_volume(base_path):
  
    file_list = []
    try:
        files = dbutils.fs.ls(base_path)
        for f in files:
            if not f.isDir():
                file_list.append(f.path)
        return file_list
    except Exception as e:
        return file_list

# COMMAND ----------

"""
    Create a Delta table in Databricks from a file located in a volume path.

    Args:
        volume_path (str): Path to the source file inside a Databricks volume
                           (e.g., '/Volumes/catalog/schema/volume_name/path/file.csv').
        catalog_name (str): Name of the catalog where the table will be created.
        schema_name (str): Name of the schema (database) where the table will be created.
        file_format (str, optional): Format of the source file (default: "csv").
        mode (str, optional): Save mode for writing the table ("overwrite" or "append", default: "overwrite").

    Returns:
        None: The function writes a Delta table to the specified catalog and schema.

    Notes:
        - Table name is derived from the file name without extension.
        - Source file must be accessible from Databricks and match the specified file format.
        - If mode is "overwrite", the existing table will be replaced.
"""
def create_table_from_volume(volume_path, catalog_name, schema_name, file_format, mode="overwrite"):
    file_name = volume_path.split("/")[-1] 
    table_base_name = file_name.rsplit(".", 1)[0]
    table_name = f"{catalog_name}.{schema_name}.{table_base_name}"
    if file_format=='csv':
        df = spark.read.format(file_format).option("header", "true").option("inferSchema", "true").load(volume_path)
    elif file_format=='json':
        df = spark.read.format(file_format).option("multiline", "true").option("inferSchema", "true").load(volume_path)
    elif file_format=='parquet':
        df = spark.read.format(file_format).load(volume_path)
    df.write.format("delta").mode(mode).saveAsTable(table_name)

# COMMAND ----------

"""
    Loads a Delta table from the specified Unity Catalog location into a Spark DataFrame.

    Args:
        catalog (str): Name of the Unity Catalog catalog containing the table.
        schema (str): Name of the schema (database) within the catalog.
        table (str): Name of the table to load.

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame containing the table's data.
"""
def create_dataframe(catalog, schema, table):
    df = spark.table(f'`{catalog}`.{schema}.{table}')
    return df

# COMMAND ----------

def create_table(df, catalog_name, schema_name, table_name):
    df.write.mode("overwrite").saveAsTable(f"`{catalog_name}`.{schema_name}.{table_name}")

# COMMAND ----------

def upsert_table(df, catalog_name, schema_name, table_name, primary_keys):
    table = DeltaTable.forName(spark, f"`{catalog_name}`.{schema_name}.{table_name}")
    condition =f"t.{primary_keys.get(table_name)} = s.{primary_keys.get(table_name)}" 
    print(condition)
    table.alias("t").merge(
        df.alias("s"),
       f"{condition}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
