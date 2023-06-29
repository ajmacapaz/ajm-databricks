# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Magic Commands for Python and SQL
# MAGIC
# MAGIC ### Available language below are
# MAGIC * **Python on cell 2**
# MAGIC * *SQL on cell 3*
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC print('Hello, World!');
# MAGIC print("Welcome to Databricks Lakehouse!");
# MAGIC print("I'm running Python!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "I'm running SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC # Variable Assignment

# COMMAND ----------

first_name = "John"
last_name = "Doe"
age = 10 + 20

print(f"Full Name: {first_name} {last_name}")
print(f"Age: {age}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Running or Calling Another Notebook

# COMMAND ----------

# MAGIC %run "../Sample/My Second Notebook"

# COMMAND ----------

print(f"Country: {country}")
print(f"State: {state}")
print(f"City: {city}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT '${country}' AS Country,
# MAGIC         '${state}' AS State,
# MAGIC         '${city}' AS City

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Extracting CSV data.

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

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM b_arsenio_j_macapaz.titles_bronze 
# MAGIC --WHERE type = 'Movie'
