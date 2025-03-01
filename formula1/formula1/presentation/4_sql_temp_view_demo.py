# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_data_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT driver_name
# MAGIC FROM race_results
# MAGIC WHERE driver_nationality = "British"

# COMMAND ----------

# MAGIC %md
# MAGIC Global Temporary View

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("glohbal_race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(driver_name)
# MAGIC FROM global_temp.glohbal_race_results
# MAGIC WHERE  position <= 2

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp