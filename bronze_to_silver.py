# Databricks notebook source
dbutils.widgets.text('bronze_catalog', 'aws-dms')
dbutils.widgets.text('bronze_schema', 'bronze')
dbutils.widgets.text('silver_catalog', 'aws-dms')
dbutils.widgets.text('silver_schema', 'silver')
bronze_catalog = dbutils.widgets.get('bronze_catalog')
bronze_schema = dbutils.widgets.get('bronze_schema')
silver_catalog = dbutils.widgets.get('silver_catalog')
silver_schema = dbutils.widgets.get('silver_schema')

# COMMAND ----------

primary_keys = {'dim_ads':'ad_id','dim_articles':'hash_key','dim_authors':'author_id','dim_users':'user_id'}
table_names = ['dim_ads','dim_articles', 'dim_authors', 'dim_users']

# COMMAND ----------

# MAGIC %run /Workspace/Users/meka.mallikharjunareddy@diggibyte.com/np_poc/common_utils

# COMMAND ----------

dim_ads = create_dataframe(bronze_catalog, bronze_schema, 'dim_ads')
dim_articles = create_dataframe(bronze_catalog, bronze_schema, 'dim_articles')
dim_authors = create_dataframe(bronze_catalog, bronze_schema, 'dim_authors')
dim_users = create_dataframe(bronze_catalog, bronze_schema, 'dim_users')

# COMMAND ----------


dim_articles = dim_articles.withColumn(
    'hash_key', 
    sha2(concat(col('article_id'), col('author_id')), 256)
)

# COMMAND ----------

dd = spark.table('`aws-dms`.silver.dim_articles')

# COMMAND ----------

display(dim_articles)

# COMMAND ----------

for i in table_names:
    df = create_dataframe(bronze_catalog, bronze_schema, i)
    df = df.dropDuplicates()
    df.dropna(subset=[f"{primary_keys.get(i)}"], how='any')
    if i=='dim_articles':
        df = df.withColumn('hash_key', sha2(concat(col('article_id'), col('author_id')), 256))
        if not spark.catalog.tableExists(f"`{silver_catalog}`.{silver_schema}.{i}"):
            create_table(df, silver_catalog, silver_schema, i)
            print(i,'create')
        else:
            upsert_table(df, silver_catalog, silver_schema, i, primary_keys)
            print(i,'upsert')
    else:
        create_table(df, silver_catalog, silver_schema, i)
        print(i,'create')

