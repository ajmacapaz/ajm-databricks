# Databricks notebook source
#%pip install --maven-coordinates "com.crealytics:spark-excel_2.12:3.3.1_0.18.7"
#databricks libraries install --maven-coordinates "com.crealytics:spark-excel_2.12:3.2.1_0.18.0"

#%pip install "com.crealytics:spark-excel_2.12:3.3.1_0.18.7"
%pip install git+https://github.com/crealytics/spark-excel
#%pip install git+https://github.com/databricks/databricks-cli

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Menu Multi-hop Test Notebook

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")
valid_dates = {"date_not_null": F.col("Date_Created_Date").isNotNull()}

filePath = "dbfs:/FileStore/team6/titles.xlsx"

df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("dataAddress", "Sheet1").load(filePath)

@dlt.table
def menu_bronze_excel():
    return (spark.read.format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("dataAddress", "Sheet1")
            .load(filePath))

    # return (spark.read.format("csv")
    #         .load(f"{source}/menu_data.csv", header = "true", inferSchema = True)
    #         .select(F.current_timestamp().alias("processing_time"),
    #                 F.input_file_name().alias("source_file"),
    #                 F.to_date(F.col("Date_Created"),"MM/dd/yyyy").alias("Date_Created_Date"),
    #                 "*"))



# @dlt.table(
#     comment = "Append only items with valid timestamps.",
#     table_properties = {"quality": "silver"}
# )

# @dlt.expect_all_or_drop(valid_dates)
# def menu_silver():
#     return (
#         dlt.read_stream("menu_bronze")
#             .select(
#                 "Category",
#                 "Item",
#                 "Date_Created_Date",
#                 "Serving_Size",
#                 "Calories",
#                 "Calories_from_Fat",
#                 "Total_Fat",
#                 "Total_Fat_%_Daily_Value",
#                 "Saturated_Fat",
#                 "Saturated_Fat_%_Daily_Value",
#                 "Trans_Fat",
#                 "Cholesterol",
#                 "Cholesterol_%_Daily_Value",
#                 "Sodium",
#                 "Sodium_%_Daily_Value",
#                 "Carbohydrates",
#                 "Carbohydrates_%_Daily_Value",
#                 "Dietary_Fiber",
#                 "Dietary_Fiber_%_Daily_Value",
#                 "Sugars",
#                 "Protein"
#             )
#     )

#@dlt.expect_all_or_drop(valid_dates)
#def menu_silver():
#    return (dtl.read_stream("menu_bronze")
#            .select("*")
#            )
    

#@dlt.table(
#    comment = "Append only items with valid timestamps.",
#    table_properties = {"quality": "silver"})

#    return (spark.readStream
#            .option("skipChangeCommits", "true")
#            .option("ignoreChanges", "true")
#            .table("menu_bronze")
#            )
            
        #dlt.read_stream("menu_bronze")
        #    .select(
        #        "Category",
        #        "Item",
        #        "Date_Created",
        #        "Date_Created_Date",
        #        "Serving_Size",
        #        "Calories",
        #        "Calories_from_Fat",
        #        "Total_Fat",
        #        "Total_Fat_%_Daily_Value",
        #        "Saturated_Fat",
        #        "Saturated_Fat_%_Daily_Value",
        #        "Trans_Fat",
        #        "Cholesterol",
        #        "Cholesterol_%_Daily_Value",
        #        "Sodium",
        #        "Sodium_%_Daily_Value",
        #        "Carbohydrates",
        #        "Carbohydrates_%_Daily_Value",
        #        "Dietary_Fiber",
        #        "Dietary_Fiber_%_Daily_Value",
        #        "Sugars",
        #        "Protein"
        #    )
    #)
