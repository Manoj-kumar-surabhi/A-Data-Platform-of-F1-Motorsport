# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 
# MAGIC Import the constrcutors.json file

# COMMAND ----------

dbutils.widgets.text("data_source", "")

v_data_source_3 = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json(f"{raw_data_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC step 2 Drop the url column

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC Step-3 Rename and create a new column called ingestion date from current_timestamp function

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed(
    "constructorId", "constructor_id"
).withColumnRenamed(
    "constructorRef", "constrcutor_ref"
).withColumn("data_source", lit(v_data_source_3)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 write data to data lake
# MAGIC

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("hive_metastore.f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")