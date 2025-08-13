# Databricks notebook source
base_path = '/Volumes/aws-dms/default/data'

# COMMAND ----------

# MAGIC %run /Workspace/Users/meka.mallikharjunareddy@diggibyte.com/np_poc/common_utils

# COMMAND ----------

table_paths = list_files_in_volume('/Volumes/aws-dms/default/data')
print(*table_paths, sep='\n')

# COMMAND ----------

for i in table_paths:
  create_table_from_volume(i, '`aws-dms`', 'bronze', 'csv')
