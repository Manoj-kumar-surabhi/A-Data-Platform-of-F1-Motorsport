# Databricks notebook source
# MAGIC %run "../includes/functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("data_source", "")

v_data_source_8 = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

qualifying_table_schema = StructType(fields=[
     StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
     StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
     StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
     StructField("q2", StringType(), True),
      StructField("q3", StringType(), True),
])

# COMMAND ----------

qualifying_df = spark.read.option("multiline", True).schema(qualifying_table_schema).json(f"/mnt/formula1/{v_file_date.strip()}/qualifying")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit


# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumnRenamed("constructorId", "constructor_id").withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source_8)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.qualify_id = src.qualify_id"
meta_store = "hive_metastore"
folder_path ="/mnt/formula1/processed/qualifying"
merge_delta_data(qualifying_final_df, meta_store, "f1_processed", "qualifying", folder_path, merge_condition, "race_id" )

# COMMAND ----------

dbutils.notebook.exit("Success")