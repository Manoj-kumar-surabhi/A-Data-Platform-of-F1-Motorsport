# Databricks notebook source
access_key = dbutils.secrets.get("formula1-group", key = "Formula1-access-key")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1surabhi.dfs.core.windows.net", access_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1surabhi.dfs.core.windows.net"))

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("formula1-group")