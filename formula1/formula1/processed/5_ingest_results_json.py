# Databricks notebook source
# MAGIC %run "../includes/functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("data_source", "")

v_source_data_5 = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

results_schema = "constructorId INT, driverId INT, fastestLap INT, fastestLapSpeed FLOAT, fastestLapTime STRING, grid INT, laps INT, milliseconds INT, number INT, points FLOAT, position INT,  positionOrder INT, positionText INT, rank INT, raceId INT, resultId INT, statusId STRING, time STRING"

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_df = spark.read.json(f"/mnt/formula1/{v_file_date}/results.json")

# Drop the duplicate column
results_df = results_df.drop("positionorder").drop("statusId")

results_df = results_df.withColumn("data_source", lit(v_source_data_5)).withColumn("file_date", lit(v_file_date))


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_final_df = results_df.withColumn("ingestion_date", current_timestamp()).withColumnRenamed("driverId", "driver_id").withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("raceId", "race_id")

# COMMAND ----------

results_final_df = results_final_df.withColumn("number", results_final_df["number"].cast("bigint")).withColumn("rank", results_final_df["rank"].cast("bigint"))



# COMMAND ----------

results_duduped_df = results_final_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC to avoid duplicate insertion of data, partition is used. It divides a table or index distinctively

# COMMAND ----------

merge_condition = "tgt.resultId = src.resultId AND tgt.race_id = src.race_id"
meta_store = "hive_metastore"
folder_path ="/mnt/formula1/processed/results"
merge_delta_data(results_duduped_df, meta_store, "f1_processed", "results", folder_path, merge_condition, "race_id" )

# COMMAND ----------

dbutils.notebook.exit("sucess")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, driver_id, count(1) from hive_metastore.f1_processed.results
# MAGIC group by race_id, driver_id
# MAGIC having count(1) > 1
# MAGIC order by race_id, driver_id desc