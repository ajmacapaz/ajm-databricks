# Databricks notebook source
print('Hello, World!');
print("Welcome to Databricks Lakehouse!");

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select "This was run using SQL.", "This is a second cell."

# COMMAND ----------

print("This is a third cell.")

# COMMAND ----------

# MAGIC %md Extracting CSV data.

# COMMAND ----------

#spark.read.format("csv")\
#            .load("dbfs:/FileStore/sample/data/menu_data.csv", header = "true", inferSchema = True)\
#            .select("saa")

menu_data = spark.read.format("csv")\
            .load("dbfs:/FileStore/team6/menu_data.csv", header = "true", inferSchema = True)
#            .select("saa")
#            .load("dbfs:/FileStore/sample/data/menu_data.csv", header = "true", inferSchema = True)
menu_data.display()

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/team6/menu_data.csv")

# COMMAND ----------

ds = spark.read\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .csv("dbfs:/FileStore/team6/menu_data.csv")

display(ds)

# COMMAND ----------

content = dbutils.fs.ls("dbfs:/FileStore/team6")
display(content)

# %fs ls dbfs:/FileStore/team6

# COMMAND ----------

dst = spark\
        .read\
        .csv("dbfs:/FileStore/team6/menu_data.csv")

# COMMAND ----------

# MAGIC %md Saving dataframe into Delta table.

# COMMAND ----------

menu_data.write.format("delta").saveAsTable("menu_nutrition_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --SELECT * FROM menu_nutrition_data
# MAGIC --DESCRIBE TABLE menu_nutrition_data
# MAGIC --DESCRIBE EXTENDED menu_nutrition_data
# MAGIC DESCRIBE DETAIL menu_nutrition_data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM menu_nutrition_data
# MAGIC WHERE Protein > 20 AND Sugars < 20
# MAGIC ORDER BY Protein DESC

# COMMAND ----------

#nutrition_df = spark.table("default.menu_nutrition_data")
nutrition_df = spark.table("b_arsenio_j_macapaz.menu_bronze")

nutrition_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --DESCRIBE DETAIL b_arsenio_j_macapaz.menu_bronze
# MAGIC
# MAGIC DESCRIBE DETAIL b_arsenio_j_macapaz.menu_bronze
# MAGIC
# MAGIC --DESCRIBE HISTORY
# MAGIC --select * from table_changes('[table_name]', 1[version number])
