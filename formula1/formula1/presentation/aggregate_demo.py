# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_result_df = spark.read.parquet(f"{presentation_data_path}/race_results").filter("race_year in (2006, 2007)")

# COMMAND ----------

from pyspark.sql.functions import countDistinct, count, sum

# COMMAND ----------

races_result_df.select(countDistinct("driver_nationality")).show()

# COMMAND ----------

races_result_df.select(sum("points")).show()

# COMMAND ----------

races_result_df.select(count("race_time")).show()

# COMMAND ----------

 races_result_df.groupBy("driver_name").agg(sum("points"), count("race_name"))

# COMMAND ----------

races_result_df = races_result_df.groupBy("race_year", "driver_name").agg(sum("points").alias("total_points"), count("race_name"))

# COMMAND ----------

display(races_result_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc, sum, count


partition_by_race_year = Window.partitionBy("race_year").orderBy(desc("total_points"))





# COMMAND ----------

display(races_result_df.withColumn("rank", rank().over(partition_by_race_year)))