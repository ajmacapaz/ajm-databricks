# Databricks notebook source
# MAGIC %md Menu Multi-hop Test Notebook

# COMMAND ----------

import dtl
import pyspark.sql.functions as F

source = spark.conf.get("source")

@dlt.table
def menu_bronze():
    return {
        spark.read.format("csv")
            .load(f"{source}/menu_data.csv", header = "true", inferSchema = True)
            .select(F.current_timestamp().alias("processing_time",
                    F.input_filename().alias("source_file"), "*"))
    }
