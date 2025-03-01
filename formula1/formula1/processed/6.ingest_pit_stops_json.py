# Databricks notebook source
# MAGIC %run "../includes/functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("data_source", "")

v_data_source_6 = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).option("multiline", "true").json(f"/mnt/formula1/{v_file_date}/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit


# COMMAND ----------

pit_stops_renamed_df = pit_stops_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source_6)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

from pyspark.sql.functions import col

# Deduplicate the source DataFrame
deduplicated_pit_stops_df = pit_stops_renamed_df.dropDuplicates(["race_id", "driver_id"])

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id"
meta_store = "hive_metastore"
folder_path = "/mnt/formula1/processed/pit_stops"

merge_delta_data(
    deduplicated_pit_stops_df,
    meta_store,
    "f1_processed",
    "pit_stops",
    folder_path,
    merge_condition,
    "race_id"
)

# COMMAND ----------

dbutils.notebook.exit("Success")