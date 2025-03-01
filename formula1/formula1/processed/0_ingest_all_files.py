# Databricks notebook source
v_result = dbutils.notebook.run("1.ingest_circuit.csv_file", 0, {"data_source": "Ergast Api", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_csv", 0, {"data_source": "Ergast Api", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors.json", 0, {"data_source": "Ergast Api", "p_file_date": "2021-04-18"})

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_json", 0, {"data_source": "Ergast Api", "p_file_date": "2021-04-18"})

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5_ingest_results_json",0, {"data_source": "Ergast Api", "p_file_date": "2021-04-18"})

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pit_stops_json",0, {"data_source": "Ergast Api", "p_file_date": "2021-04-18"})

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_folder",0, {"data_source": "Ergast Api", "p_file_date": "2021-04-18"})

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_folder",0, {"data_source": "Ergast Api", "p_file_date": "2021-04-18"})

v_result

# COMMAND ----------

