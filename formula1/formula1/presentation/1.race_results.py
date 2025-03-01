# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_data_path}/circuits")
circuits_df = circuits_df.select("circuit_id", "location")


# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_data_path}/races")
races_df = races_df.select("race_id", "race_year", "race_name","race_date","circuit_id")

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_data_path}/drivers")
drivers_df = drivers_df.select("driver_id", "name","number","nationality")
drivers_df = drivers_df.withColumnRenamed("number", "driver_number").withColumnRenamed("name", "driver_name").withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_data_path}/constructors")
constructors_df = constructors_df.select("constructor_id", "name")
constructors_df =constructors_df.withColumnRenamed("name", "team")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_data_path}/results") \
    .filter(f"file_date = '{v_file_date}'") \
    .select("grid", "fastestLap", "time", "points", "driver_id", "constructor_id", "race_id", "position") \
    .withColumnRenamed("time", "race_time") 



# COMMAND ----------

# Corrected code
race_results_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
                            .drop("circuit_id")

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

race_results_final_df = (
    results_df.join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner")
              .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")
              .join(race_results_df, results_df.race_id == race_results_df.race_id, "inner")
              .drop(race_results_df.race_id,)
)

race_results_final_df = race_results_final_df.drop("constructor_id", "driver_id").withColumn("file_date", lit(v_file_date))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

race_results_final_df = race_results_final_df.withColumn("created_date", current_timestamp())

# COMMAND ----------

#overwrite_partition(race_results_final_df, "race_id", "f1_presentation", "race_results")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
meta_store = "hive_metastore"
folder_path ="/mnt/formula1/presentation/results"
merge_delta_data(race_results_final_df, meta_store, "f1_presentation", "results", folder_path, merge_condition, "race_id" )