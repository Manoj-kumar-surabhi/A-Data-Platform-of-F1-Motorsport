# Databricks notebook source
# MAGIC %md
# MAGIC #Access Azure data lake using service principal
# MAGIC
# MAGIC #steps to follow
# MAGIC 1.Register Azure AD Application/ Service Principal
# MAGIC 2.Generate a secret/password for the application
# MAGIC 3.set spark config with APP/client Id, Directory/Tenant id and secret
# MAGIC 4.Assign Role 'Storage blob data contributor" to the data lake
# MAGIC

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1surabhi.dfs.core.windows.net", "vVDkoXHxryRWJj5/ttts404XFhw0cQautIPkjmo3/qlOv63qW5k++XkqkEegM+oauNwzQYWUsgT1+AStXNAwcg==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1surabhi.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1surabhi.dfs.core.windows.net/circuits.csv"))