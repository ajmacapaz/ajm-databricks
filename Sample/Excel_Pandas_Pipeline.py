# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Bronze Layer

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
import pandas as pd
from pyspark.sql.functions import (regexp_replace, col)
from pyspark.sql.types import (StructField, StringType, StructType, IntegerType)


filesource = spark.conf.get("filesource")

credits_schema = StructType(fields = [StructField('person_id', IntegerType(), True),
                  StructField('id', StringType(), True),
                  StructField('name', StringType(), True),
                  StructField('character', StringType(), True),
                  StructField('role', StringType(), True)])

@dlt.table (
    name = "subscribers_bronze",
    comment = "All records must be there in bronze table",
    table_properties = {"quality":"bronze"}   
)

def subscribers_bronze():
    pdf = pd.read_excel(f"{filesource}/Subscribers.xlsx")
    return (spark.createDataFrame(pdf))

@dlt.table(
    name = "titles_bronze",
    comment= "All records must be there in bronze table",
    table_properties={"quality": "bronze"})

def titles_bronze():
    pdf = pd.read_excel(f"{filesource}/titles.xlsx")
    return (spark.createDataFrame(pdf))

@dlt.table(
    name = "credits_bronze",
    comment = "All records must be there in bronze table",
    table_properties={"quality": "bronze"})

def credits_bronze():
    pdf = pd.read_excel(f"{filesource}/credits.xlsx", dtype=str)
    sdf = spark.createDataFrame(pdf)
    return (sdf.withColumn("person_id", regexp_replace(col("person_id"), "\'", "").cast("int")))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Silver Layer

# COMMAND ----------

@dlt.table(
    name = "title_subscribers_silver",
    comment = "Combine all the data between title and subscribers by production_countries.",
    table_properties={"quality": "silver"}
)

def title_subscribers_silver():
    return (dlt.read_stream("titles_bronze")
            .join(dlt.read("subscribers_bronze"), ["production_countries"]))
            #.where(F.col("genres") != "[]"))
    
@dlt.table(
    name = "title_credits_silver",
    comment = "Combine all the data between title and credits by id.",
    table_properties={"quality": "silver"}
)

def title_credits_silver():
    return (dlt.read_stream("titles_bronze")
            .join(dlt.read("credits_bronze"), ["id"]))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Gold Layer

# COMMAND ----------

@dlt.table(
    name = "genre_gold",
    comment = "Group by Type",
    table_properties={"quality": "gold"}
)
def genre_gold():
    df = dlt.read("title_subscribers_silver")
    return (df.groupBy(F.col("genres"), F.col("type"))
            .agg(F.count("*").alias("count"))
            .orderBy(F.count("genres").desc(), F.col("type"))
            )

@dlt.table(
    name = "subscribersByCountry_gold",
    comment = "Group by Country",
    table_properties={"quality": "gold"}
)
def subscribersByCountry_gold():
    df = dlt.read("title_subscribers_silver")
    return (df.groupBy(F.col("production_countries"))
            .agg(F.count("Subscribers").alias("total_subscribers"))
            .orderBy(F.count("Subscribers").desc())
            )


