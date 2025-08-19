# Databricks notebook source
from pyspark.sql import Window

# COMMAND ----------

dbutils.widgets.text('gold_catalog', 'aws-dms')
dbutils.widgets.text('gold_schema', 'gold')
dbutils.widgets.text('silver_catalog', 'aws-dms')
dbutils.widgets.text('silver_schema', 'silver')
gold_catalog = dbutils.widgets.get('gold_catalog')
gold_schema = dbutils.widgets.get('gold_schema')
silver_catalog = dbutils.widgets.get('silver_catalog')
silver_schema = dbutils.widgets.get('silver_schema')

# COMMAND ----------

silver_table_names = ['dim_ads','dim_articles', 'dim_authors', 'dim_users']
gold_table_names = ['fact_article_engagement', 'fact_ad_performance']
primary_keys = {'dim_ads':'dim_ads_sk_id','dim_articles':'dim_articles_sk_id','dim_authors':'dim_authors_sk_id','dim_users':'dim_users_sk_id','fact_article_engagement':'fact_article_engagement_hash_key', 'fact_ad_performance':'fact_ad_performance_hash_key'}

# COMMAND ----------

# MAGIC %run /Workspace/Users/meka.mallikharjunareddy@diggibyte.com/np_poc/common_utils

# COMMAND ----------

dim_ads = create_dataframe('aws-dms', 'silver', 'dim_ads')
dim_articles = create_dataframe('aws-dms', 'silver', 'dim_articles')
dim_authors = create_dataframe('aws-dms', 'silver', 'dim_authors')
dim_users = create_dataframe('aws-dms', 'silver', 'dim_users')

# COMMAND ----------

from pyspark.sql.functions import rand, floor, round as spark_round, when, current_timestamp, col

# ------------------------------
# Transformation Helpers
# ------------------------------

def generate_read_time(df):
    """Random read time in seconds (0–300)."""
    return df.withColumn("read_time_seconds", floor(rand() * 300))

def generate_scroll_depth(df):
    """Random scroll depth % (0–100)."""
    return df.withColumn("scroll_depth", spark_round(rand() * 100, 2))

def generate_clicked_ad(df):
    """Click flag: 1 if random > 0.7 else 0."""
    return df.withColumn("clicked_ad", when(rand() > 0.7, 1).otherwise(0))

def Hash_key(df):
    return df.withColumn("fact_article_engagement_hash_key", sha2(concat(df["article_id"], df["user_id"], df['ad_id']), 256))


def random_assign_ads(articles_df, ads_df):
    """
    Assign ads randomly (~50% chance) by generating a temp join key.
    Only rows where random > 0.5 will be joined.
    """
    ads_flag = ads_df.withColumn("ads_flag", when(rand() > 0.5, 1).otherwise(0))
    articles_flagged = articles_df.withColumn("ads_flag", when(rand() > 0.5, 1).otherwise(0))
    return articles_flagged.join(ads_flag, on="ads_flag", how="left").drop("ads_flag")

# ------------------------------
# Main Transformation Function
# ------------------------------

def transform_user_engagement(users_df, articles_df, authors_df, ads_df):
    # Cross join users & articles
    engagement_df = users_df.crossJoin(articles_df)

    # Join authors
    engagement_df = engagement_df.join(
        authors_df,
        on=engagement_df['author_id'] == authors_df['author_id'],
        how="left"
)

    # Join ads (~50% chance) — simulate by flag join
    ads_flagged = ads_df.withColumn("ads_flag", when(rand() > 0.5, 1).otherwise(0))
    engagement_flagged = engagement_df.withColumn("ads_flag", when(rand() > 0.5, 1).otherwise(0))
    engagement_df = engagement_flagged.join(ads_flagged, on="ads_flag", how="left").drop("ads_flag")

    # Add metrics & rename columns to match SQL output
    engagement_df = (
        engagement_df
        .withColumn("event_timestamp", current_timestamp())
        .transform(generate_read_time)
        .transform(generate_scroll_depth)
        .transform(generate_clicked_ad)
        .transform(Hash_key)
        .withColumnRenamed("language", "article_language")
        .withColumnRenamed("name", "author_name")
    )

    # Select columns in final order
    engagement_df = engagement_df.select(
        col("dim_users_sk_id"),
        col("dim_articles_sk_id"),
        col("event_timestamp"),
        col("read_time_seconds"),
        col("scroll_depth"),
        col("clicked_ad"),
        col("category"),
        col("location"),
        col("article_language"),
        col("author_name"),
        col("dim_ads_sk_id"),
        col("ad_type"),
        col("advertiser"),
        col('fact_article_engagement_hash_key')
    )

    return engagement_df

# ------------------------------
# Example Usage
# ------------------------------

fact_article_engagement_df = transform_user_engagement(dim_users, dim_articles, dim_authors, dim_ads)

# COMMAND ----------

from pyspark.sql.functions import rand, sha2, concat, col, current_date, round as spark_round

# Generate synthetic metrics using Spark's rand()
def generate_impressions(df):
    return df.withColumn('impression', spark_round(rand() * 1000, 0))

def generate_clicks(df):
    return df.withColumn('clicks', spark_round(rand() * 500, 0))

def generate_cost(df):
    return df.withColumn('cost', spark_round(rand() * 200, 2))

def generate_revenue(df):
    return df.withColumn('revenue', spark_round(rand() * 400, 2))

def hash_key(df):
    return df.withColumn("fact_ad_performance_hash_key", sha2(concat(col("article_id"), col("ad_id")), 256))


# COMMAND ----------

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

from pyspark.sql.functions import when

def calc_ctr(df):
    return df.withColumn(
        'calc_ctr',
        when(col('impression') > 0, spark_round(col('clicks') / col('impression'), 4)).otherwise(0.0)
    )

def calc_cpc(df):
    return df.withColumn(
        'calc_cpc',
        when(col('clicks') > 0, spark_round(col('cost') / col('clicks'), 2)).otherwise(0.0)
    )

def calc_rpm(df):
    return df.withColumn(
        'calc_rpm',
        when(col('impression') > 0, spark_round((col('revenue') / col('impression')) * 1000, 2)).otherwise(0.0)
    )


# COMMAND ----------

def add_kpis(df):
    """Add CTR, CPC, RPM metrics."""
    return (
        df.transform(calc_ctr)
          .transform(calc_cpc)
          .transform(calc_rpm)
    )


# COMMAND ----------

fact_ad_performance_df = (
    dim_ads.crossJoin(dim_articles)
           .join(dim_authors, dim_articles['dim_articles_sk_id'] == dim_authors['dim_authors_sk_id'], "inner")
           .transform(add_base_metrics)
           .transform(add_kpis)
           .withColumnRenamed("name", "author_name")
           .withColumn('advertiser', lit('*****'))
           .withColumn('author_name', lit('******'))
)


# COMMAND ----------

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
        df.write.mode("overwrite").partitionBy('advertiser', 'author_name').saveAsTable(f"`{gold_catalog}`.{gold_schema}.{i}") 
        spark.sql(query)
        print('create')
    else:
        upsert_table(df, gold_catalog, gold_schema, i, primary_keys)
        print('update')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM `aws-dms`.gold.fact_article_engagement

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM `aws-dms`.gold.fact_ad_performance
