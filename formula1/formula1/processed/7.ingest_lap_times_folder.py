# Databricks notebook source
# MAGIC %run "../includes/functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("data_source", "")

v_data_source_7 = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
     StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"/mnt/formula1/{v_file_date}/lap_times")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit


# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source_7)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"
meta_store = "hive_metastore"
folder_path ="/mnt/formula1/processed/lap_times"
merge_delta_data(lap_times_final_df, meta_store, "f1_processed", "lap_times", folder_path, merge_condition, "race_id" )

# COMMAND ----------

dbutils.notebook.exit("Success")