# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

df= spark.read.parquet(f"{processed_data_path}/races")

# COMMAND ----------

display(df)

# COMMAND ----------

filtered_df = df.filter("race_year = 2001")
display(filtered_df)