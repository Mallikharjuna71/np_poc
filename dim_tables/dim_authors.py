# Databricks notebook source
dbutils.widgets.text('bronze_catalog', 'aws-dms')
dbutils.widgets.text('bronze_schema', 'bronze')
dbutils.widgets.text('silver_catalog', 'aws-dms')
dbutils.widgets.text('silver_schema', 'silver')
dbutils.widgets.text('table_name', 'dim_authors')
table_name = dbutils.widgets.get('table_name')
bronze_catalog = dbutils.widgets.get('bronze_catalog')
bronze_schema = dbutils.widgets.get('bronze_schema')
silver_catalog = dbutils.widgets.get('silver_catalog')
silver_schema = dbutils.widgets.get('silver_schema')

# COMMAND ----------

generated_keys = {'dim_ads':'dim_ads_sk_id','dim_articles':'dim_articles_sk_id','dim_authors':'dim_authors_sk_id','dim_users':'dim_users_sk_id','fact_article_engagement':'hash_key', 'fact_ad_performance':'hash_key'}
primary_keys = {'dim_ads':'ad_id','dim_articles':'article_id','dim_authors':'author_id','dim_users':'user_id','fact_article_engagement':'hash_key', 'fact_ad_performance':'hash_key'}

# COMMAND ----------

# MAGIC %run /Workspace/Users/meka.mallikharjunareddy@diggibyte.com/np_poc/common_utils

# COMMAND ----------

df = create_dataframe(bronze_catalog, bronze_schema, table_name)

# COMMAND ----------

df = df.withColumn(f'{table_name}_hash_key', sha2(concat_ws(primary_keys[table_name]), 256))

# COMMAND ----------

df = generate_int_key_from_string(df,table_name, 'hash_key')

# COMMAND ----------

silver_table(df, table_name, silver_catalog, silver_schema, generated_keys)
