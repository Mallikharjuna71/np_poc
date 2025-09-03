# Databricks notebook source
base_path = '/Volumes/aws-dms/default/data'

# COMMAND ----------

# MAGIC %run /Workspace/Users/meka.mallikharjunareddy@diggibyte.com/np_poc/common_utils

# COMMAND ----------

table_paths = list_files_in_volume(base_path)

# COMMAND ----------

for i in table_paths:
  create_table_from_volume(i, '`aws-dms`', 'bronze', 'csv')
