# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_data_path}/circuits").withColumnRenamed("name","circuits_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_data_path}/races").withColumnRenamed("name","race_name")

races_df = races_df.filter("race_year == 2001 or race_year == 2003")

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"inner")\
.select(circuits_df.circuits_name, circuits_df.location, circuits_df.country, races_df.race_year, races_df.race_name)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

race_circuit_left_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left").select(circuits_df.circuits_name, circuits_df.location, circuits_df.country, races_df.race_year, races_df.race_name)

# COMMAND ----------

display(race_circuit_left_df)

# COMMAND ----------

race_circuit_right_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right").select(circuits_df.circuits_name, circuits_df.location, circuits_df.country, races_df.race_year, races_df.race_name)

# COMMAND ----------

display(race_circuit_right_df)

# COMMAND ----------

race_circuit_left_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full").select(circuits_df.circuits_name, circuits_df.location, circuits_df.country, races_df.race_year, races_df.race_name)

# COMMAND ----------

display(race_circuit_left_df)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###semi join
# MAGIC It only displays the left side table rows which matches the condition with the right table
# MAGIC

# COMMAND ----------

race_circuit_semi_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

display(race_circuit_semi_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Anti join
# MAGIC It is opposite of semi but returns the left table values that are not matched from the right

# COMMAND ----------

race_circuit_anti_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(race_circuit_anti_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## cross join
# MAGIC it multiplies the two tables

# COMMAND ----------

race_circuit_cross_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "cross")

# COMMAND ----------

display(race_circuit_cross_df)