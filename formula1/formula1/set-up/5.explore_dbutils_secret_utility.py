# Databricks notebook source
# MAGIC %md
# MAGIC %Explore the capabilities of dbutils.secrets utility
# MAGIC

# COMMAND ----------

dbutils.secrets.help( )

# COMMAND ----------

 dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list()