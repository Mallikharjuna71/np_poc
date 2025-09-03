# Databricks notebook source
# DBTITLE 1,imports
from pyspark.sql import Window
from pyspark.sql.functions import rand, floor, round as spark_round, when, current_timestamp, col
from pyspark.sql.functions import rand, sha2, concat, col, current_date, round as spark_round
from pyspark.sql.functions import when
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import lit



# COMMAND ----------

# DBTITLE 1,widgets
dbutils.widgets.text('gold_catalog', 'aws-dms')
dbutils.widgets.text('gold_schema', 'gold')
dbutils.widgets.text('silver_catalog', 'aws-dms')
dbutils.widgets.text('silver_schema', 'silver')
gold_catalog = dbutils.widgets.get('gold_catalog')
gold_schema = dbutils.widgets.get('gold_schema')
silver_catalog = dbutils.widgets.get('silver_catalog')
silver_schema = dbutils.widgets.get('silver_schema')

# COMMAND ----------

# DBTITLE 1,variables
silver_table_names = ['dim_ads','dim_articles', 'dim_authors', 'dim_users']
gold_table_names = ['fact_article_engagement', 'fact_ad_performance']
primary_keys = {'dim_ads':'dim_ads_sk_id','dim_articles':'dim_articles_sk_id','dim_authors':'dim_authors_sk_id','dim_users':'dim_users_sk_id','fact_article_engagement':'fact_article_engagement_hash_key', 'fact_ad_performance':'fact_ad_performance_hash_key'}
dbfs_path = '/Volumes/aws-dms/default/gold'

# COMMAND ----------

# MAGIC %run /Workspace/Users/meka.mallikharjunareddy@diggibyte.com/np_poc/common_utils

# COMMAND ----------

# DBTITLE 1,silver tables
dim_ads = create_dataframe('aws-dms', 'silver', 'dim_ads')
dim_articles = create_dataframe('aws-dms', 'silver', 'dim_articles')
dim_authors = create_dataframe('aws-dms', 'silver', 'dim_authors')
dim_users = create_dataframe('aws-dms', 'silver', 'dim_users')

# COMMAND ----------

# DBTITLE 1,selected columns
columns = [
    "dim_users_sk_id",
    "dim_articles_sk_id",
    "event_timestamp",
    "read_time_seconds",
    "scroll_depth",
    "clicked_ad",
    "category",
    "location",
    "article_language",
    "author_name",
    "dim_ads_sk_id",
    "ad_type",
    "advertiser",
    "fact_article_engagement_hash_key"
]


# COMMAND ----------

# DBTITLE 1,Hash_key
def Hash_key(df):
    return df.withColumn("fact_article_engagement_hash_key", sha2(concat(df["article_id"], df["user_id"], df['ad_id']), 256))

# COMMAND ----------

# DBTITLE 1,joins
def perform_joins(users_df, articles_df, authors_df, ads_df):
    """Perform all joins: users+articles, authors, ads."""
    # Cross join users & articles
    engagement_df = users_df.crossJoin(articles_df)

    # Join authors
    engagement_df = engagement_df.join(
        authors_df,
        on=engagement_df["author_id"] == authors_df["author_id"],
        how="left"
    )

    # Join ads (~50% chance)
    ads_flagged = ads_df.transform(add_random_flag)
    engagement_flagged = engagement_df.transform(add_random_flag)
    engagement_df = engagement_flagged.join(ads_flagged, on="ads_flag", how="left").drop("ads_flag")

    return engagement_df

# COMMAND ----------

# DBTITLE 1,transformations
def apply_transformations(df, columns):
    """Apply all transformations: metrics, renames, selects."""
    return (
        df.withColumn("event_timestamp", current_timestamp())
          .transform(generate_read_time)
          .transform(generate_scroll_depth)
          .transform(generate_clicked_ad)
          .transform(Hash_key)
          .withColumnRenamed("language", "article_language")
          .withColumnRenamed("name", "author_name")
          .select(*columns)
    )

# COMMAND ----------

# DBTITLE 1,master function
def transform_user_engagement(users_df, articles_df, authors_df, ads_df, columns):
    joined_df = perform_joins(users_df, articles_df, authors_df, ads_df)
    transformed_df = apply_transformations(joined_df, columns)
    return transformed_df

# COMMAND ----------

# DBTITLE 1,fact_article_engagement_df
fact_article_engagement_df = transform_user_engagement(
    dim_users, dim_articles, dim_authors, dim_ads, columns
)

# COMMAND ----------

# DBTITLE 1,hash_key
def hash_key(df):
    return df.withColumn("fact_ad_performance_hash_key", sha2(concat(col("article_id"), col("ad_id")), 256))

# COMMAND ----------

# DBTITLE 1,add_base_metrics
def add_base_metrics(df):
    """Add impressions, clicks, cost, revenue."""
    return (
        df.withColumn("event_date", current_date())
          .transform(hash_key)
          .transform(generate_impressions)
          .transform(generate_clicks)
          .transform(generate_cost)
          .transform(generate_revenue)
    )


# COMMAND ----------

# DBTITLE 1,add_kpis
def add_kpis(df):
    """Add CTR, CPC, RPM metrics."""
    return (
        df.transform(calc_ctr)
          .transform(calc_cpc)
          .transform(calc_rpm)
    )

# COMMAND ----------

# DBTITLE 1,perform_ad_joins

def perform_ad_joins(dim_ads, dim_articles, dim_authors):
    """Perform all joins for fact_ad_performance."""
    return (
        dim_ads.crossJoin(dim_articles)
               .join(
                   dim_authors,
                   dim_articles["dim_articles_sk_id"] == dim_authors["dim_authors_sk_id"],
                   "inner"
               )
    )

# COMMAND ----------

# DBTITLE 1,apply_ad_transformations
def apply_ad_transformations(df):
    """Apply all transformations: metrics, KPIs, renames, literals."""
    return (
        df.transform(add_base_metrics)
          .transform(add_kpis)
          .withColumnRenamed("name", "author_name")
          .withColumn("advertiser", lit("*****"))
          .withColumn("author_name", lit("******"))
    )

# COMMAND ----------

# DBTITLE 1,transform_ad_performance
def transform_ad_performance(dim_ads, dim_articles, dim_authors):
    joined_df = perform_ad_joins(dim_ads, dim_articles, dim_authors)
    transformed_df = apply_ad_transformations(joined_df)
    return transformed_df

# COMMAND ----------

# DBTITLE 1,fact_ad_performance_df
fact_ad_performance_df = transform_ad_performance(dim_ads, dim_articles, dim_authors)

# COMMAND ----------

# DBTITLE 1,selected columns
columns = [
    "dim_ads_sk_id",
    "ad_type",
    "advertiser",
    "format",
    "dim_articles_sk_id",
    "title",
    "category",
    "dim_articles.author_id", 
    "publish_date",
    "language",
    "fact_ad_performance_hash_key",
    "author_name",
    "department",
    "language_specialization",
    "event_date",
    "impression",
    "clicks",
    "cost",
    "revenue",
    "calc_ctr",
    "calc_cpc",
    "calc_rpm"
]


# COMMAND ----------

# DBTITLE 1,fact_ad_performance_df
fact_ad_performance_df = fact_ad_performance_df.select(*columns)

# COMMAND ----------

for i in gold_table_names:
    if i == 'fact_ad_performance':
        df = fact_ad_performance_df
        query = f""" OPTIMIZE `{gold_catalog}`.{gold_schema}.{i} ZORDER BY (dim_ads_sk_id, dim_articles_sk_id)"""
    elif i == 'fact_article_engagement':
        df = fact_article_engagement_df
        query = f""" OPTIMIZE `{gold_catalog}`.{gold_schema}.{i} ZORDER BY (dim_users_sk_id, dim_articles_sk_id)"""
    if not spark.catalog.tableExists(f"`{gold_catalog}`.{gold_schema}.{i}"):
        df.write.mode("overwrite").format("delta").partitionBy("advertiser", "author_name").save(f"{dbfs_path}/{i}")
        # spark.sql(query)
    else:
        upsert_table(df, gold_catalog, gold_schema, i, primary_keys)

