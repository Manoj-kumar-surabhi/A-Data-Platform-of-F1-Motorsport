# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_list = spark.read.format('delta').load(f"{presentation_data_path}/results").filter(f"file_date = '{v_file_date}'").select("race_year").distinct().collect()

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)




# COMMAND ----------

from pyspark.sql.functions import col
race_results_df = spark.read.format('delta').load(f"{presentation_data_path}/results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

driver_standings_df = race_results_df

# COMMAND ----------

from pyspark.sql.functions import sum, when,count

# COMMAND ----------

from pyspark.sql.functions import sum, when, col

driver_standings_df = driver_standings_df.groupBy(
    "race_year", 
    "driver_nationality", 
    "driver_name", 
    "team"
).agg(
    sum("points").alias("total_points"), 
    count(when(col("position") == 1, 1).otherwise(0)).alias("wins")
)

display(driver_standings_df)

# COMMAND ----------

from pyspark.sql.functions import desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

partition_by_year = Window.orderBy(desc("total_points"), desc("wins")).partitionBy("race_year")

driver_standings_df = driver_standings_df.withColumn("rank", rank().over(partition_by_year))

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.team = src.team"
meta_store = "hive_metastore"
folder_path ="/mnt/formula1/presentation/driver_standings"
merge_delta_data(driver_standings_df, meta_store, "f1_presentation", "driver_standings", folder_path, merge_condition, "driver_name" )