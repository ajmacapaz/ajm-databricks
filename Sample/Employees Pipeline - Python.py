# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Bronze Layer

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")

@dlt.table
def employees_bronze():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{source}/temp")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )

@dlt.table(
    comment = "Streaming data from bronze to silver.",
    table_properties = {"quality": "silver"}
)
def employees_silver_streaming():
    return (dlt.read_stream("employees_bronze")
            .select("EmployeeNo",
                    "LastName",
                    "FirstName",
                    "Age",
                    "EmployementStatus"
                    )
    )

@dlt.table(
    comment = "Live table (Materialize) data from bronze to silver.",
    table_properties = {"quality": "silver"}
)
def employees_silver_materialize():
    return (dlt.read("employees_bronze")
            .select("EmployeeNo",
                    "LastName",
                    "FirstName",
                    "EmployementStatus"
                    )
    )




