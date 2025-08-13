# Databricks notebook source
table_names = ['dim_ads','dim_articles', 'dim_authors', 'dim_users']
primary_keys = {'dim_ads':'ad_id','dim_articles':'hash_id','dim_authors':'author_id','dim_users':'user_id'}

# COMMAND ----------

# MAGIC %run /Workspace/Users/meka.mallikharjunareddy@diggibyte.com/np_poc/common_utils

# COMMAND ----------

dim_ads = create_dataframe('aws-dms', 'silver', 'dim_ads')
dim_articles = create_dataframe('aws-dms', 'silver', 'dim_articles')
dim_authors = create_dataframe('aws-dms', 'silver', 'dim_authors')
dim_users = create_dataframe('aws-dms', 'silver', 'dim_users')

# COMMAND ----------

display(dim_ads)
