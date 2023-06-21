# Databricks notebook source
# MAGIC %md Displaying List of Files from /Filestore/team6 dbfs

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/team6/"))
#%fs ls dbfs:/FileStore/team6/
display(dbutils.fs.ls("dbfs:/FileStore/team6/netflix"))

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/team6/menu_data.csv")

# COMMAND ----------

ds = spark\
    .read\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .csv("dbfs:/FileStore/team6/menu_data.csv")

display(ds)

# COMMAND ----------

display(
    ds.describe("Category", "Date_Created")
)

# COMMAND ----------

dsNoNulls = ds.na.drop("any")

display(dsNoNulls)

# COMMAND ----------

display(
    dsNoNulls
        #.where("Date_Created = 'Blah'")
        .filter($"Date_Created") > 0)
)
