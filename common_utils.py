# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

def list_files_in_volume(base_path):
    """
    List all files in a given Databricks volume path (non-recursive).

    Args:
        base_path (str): Path to the base directory inside a Databricks volume
                         (e.g., '/Volumes/catalog/schema/volume_name/path').

    Returns:
        list: A list of file paths (strings) located directly in the given base path.
              Directories are excluded. If an error occurs, an empty list is returned.

    """
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

def create_table_from_volume(volume_path, catalog_name, schema_name, file_format, mode="overwrite"):
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

    """
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

def create_dataframe(catalog, schema, table):
    """
    Loads a Delta table from the specified Unity Catalog location into a Spark DataFrame.

    Args:
        catalog (str): Name of the Unity Catalog catalog containing the table.
        schema (str): Name of the schema (database) within the catalog.
        table (str): Name of the table to load.

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame containing the table's data.
    """
    df = spark.table(f'`{catalog}`.{schema}.{table}')
    return df

# COMMAND ----------

def create_table(df, catalog_name, schema_name, table_name):
    """
    Create or overwrite a Delta table in Databricks from a given DataFrame.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame to be written as a table.
        catalog_name (str): Name of the catalog in which the table will be created.
        schema_name (str): Name of the schema (database) under the catalog.
        table_name (str): Name of the target table to create.

    Returns:
        None
    """
    df.write.mode("overwrite").saveAsTable(f"`{catalog_name}`.{schema_name}.{table_name}")

# COMMAND ----------

def upsert_table(df, catalog_name, schema_name, table_name, primary_keys):
    """
    Perform an upsert (merge) operation into a Delta table in Databricks.

    Args:
        df (pyspark.sql.DataFrame): 
            Source DataFrame containing new or updated records.
        catalog_name (str): 
            Name of the catalog where the target table resides.
        schema_name (str): 
            Name of the schema (database) where the target table resides.
        table_name (str): 
            Name of the Delta table to be upserted into.
        primary_keys (dict): 
            A dictionary mapping table names to their primary key column names.
            Example: {"dim_articles": "article_id", "dim_users": "user_id"}

    Returns:
        None
    """
    table = DeltaTable.forName(spark, f"`{catalog_name}`.{schema_name}.{table_name}")
    condition =f"t.{primary_keys.get(table_name)} = s.{primary_keys.get(table_name)}" 
    print(condition)
    table.alias("t").merge(
        df.alias("s"),
       f"{condition}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

def silver_table(df,table_name, silver_catalog, silver_schema, primary_keys):
    """
    Create or update a Silver layer Delta table in Databricks.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame to be written to the Silver layer.
        table_name (str): Name of the Silver table to create or update.
        silver_catalog (str): Target catalog for the Silver table.
        silver_schema (str): Target schema (database) for the Silver table.
        primary_keys (list[str]): List of primary key column names used for upsert operations.

    Returns:
        None
    """
    if table_name == 'dim_articles_updated':
        if not spark.catalog.tableExists(f"`{silver_catalog}`.{silver_schema}.{table_name}"):
            create_table(df, silver_catalog, silver_schema, table_name)
        else:
            upsert_table(df, silver_catalog, silver_schema, table_name, primary_keys)
    else:
        create_table(df, silver_catalog, silver_schema, table_name)


# COMMAND ----------

def generate_int_key_from_string(df, table_name, col_name):
    """
    Generate a surrogate integer key column from a string column in a given DataFrame.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame containing the source column.
        table_name (str): Logical table name prefix (used to construct the surrogate key column name).
        col_name (str): Name of the string column from which the surrogate key will be generated.

    Returns:
        pyspark.sql.DataFrame: DataFrame with an additional surrogate key column named
                               '{table_name}_sk_id', generated using row_number().
    """
    window = Window.partitionBy(f"{table_name}_{col_name}").orderBy(col(f"{table_name}_{col_name}"))
    df = df.withColumn(f"{table_name}_sk_id", row_number().over(window)).filter(col(f"{table_name}_sk_id") == 1)
    return df
    

# COMMAND ----------

def generate_read_time(df):
    """Random read time in seconds (0–300)."""
    return df.withColumn("read_time_seconds", floor(rand() * 300))

# COMMAND ----------

def generate_scroll_depth(df):
    """Random scroll depth % (0–100)."""
    return df.withColumn("scroll_depth", spark_round(rand() * 100, 2))

# COMMAND ----------

def generate_clicked_ad(df):
    """Click flag: 1 if random > 0.7 else 0."""
    return df.withColumn("clicked_ad", when(rand() > 0.7, 1).otherwise(0))

# COMMAND ----------

from pyspark.sql.functions import rand, when

def add_random_flag(df):
    """
    Add a random flag column to a DataFrame.

    Args:
        df (DataFrame): Input Spark DataFrame
        col_name (str): Name of the new flag column (default = "flag")
        threshold (float): Threshold for assigning true_val (default = 0.5)
        true_val (any): Value to assign when rand() > threshold (default = 1)
        false_val (any): Value to assign otherwise (default = 0)

    Returns:
        DataFrame: DataFrame with new random flag column
    """
    return df.withColumn(
        'ads_flag',
        when(rand() > 0.5, 1).otherwise(0)
    )


# COMMAND ----------

def generate_impressions(df):
    return df.withColumn('impression', spark_round(rand() * 1000, 0))

# COMMAND ----------

def generate_clicks(df):
    return df.withColumn('clicks', spark_round(rand() * 500, 0))

# COMMAND ----------

def generate_cost(df):
    return df.withColumn('cost', spark_round(rand() * 200, 2))

# COMMAND ----------

def generate_revenue(df):
    return df.withColumn('revenue', spark_round(rand() * 400, 2))

# COMMAND ----------

def calc_ctr(df):
    return df.withColumn(
        'calc_ctr',
        when(col('impression') > 0, spark_round(col('clicks') / col('impression'), 4)).otherwise(0.0)
    )

# COMMAND ----------

def calc_cpc(df):
    return df.withColumn(
        'calc_cpc',
        when(col('clicks') > 0, spark_round(col('cost') / col('clicks'), 2)).otherwise(0.0)
    )

# COMMAND ----------

def calc_rpm(df):
    return df.withColumn(
        'calc_rpm',
        when(col('impression') > 0, spark_round((col('revenue') / col('impression')) * 1000, 2)).otherwise(0.0)
    )
