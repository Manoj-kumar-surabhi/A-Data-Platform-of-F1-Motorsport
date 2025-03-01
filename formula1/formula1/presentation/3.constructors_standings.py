# Databricks notebook source
# MAGIC %run "../includes/functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results = spark.read.format('delta').load(f"{presentation_data_path}/results").filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results, "race_year")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_results_df = (
    spark.read.format('delta').load(f"{presentation_data_path}/results")
    .filter(col("race_year").isin(race_year_list))
)

# COMMAND ----------

from pyspark.sql.functions import sum, rank, when, count, desc
from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

constructor_standings = race_results_df.groupBy("race_year", "team").agg(
    sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("win_count")
)


# COMMAND ----------

partition_by_year = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("win_count"))

constructor_standings = constructor_standings.withColumn("rank", rank().over(partition_by_year))

# COMMAND ----------

# Cast the race_year column to int
constructor_standings = constructor_standings.withColumn("race_year", constructor_standings["race_year"].cast("int"))



# COMMAND ----------

merge_condition = "tgt.race_year  = src.race_year"
meta_store = "hive_metastore"
folder_path ="/mnt/formula1/presentation/constructor_standings"
merge_delta_data(constructor_standings, meta_store, "f1_presentation", "constructor_standings", folder_path, merge_condition, "race_year" )