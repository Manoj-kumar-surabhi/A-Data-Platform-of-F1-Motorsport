# Databricks notebook source
formula1_demo_sas_token = dbutils.secrets.get("formula1-group",key="demo-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1surabhi.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1surabhi.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1surabhi.dfs.core.windows.net", formula1_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1surabhi.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1surabhi.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------



# COMMAND ----------

dbutils.secrets.listScopes()