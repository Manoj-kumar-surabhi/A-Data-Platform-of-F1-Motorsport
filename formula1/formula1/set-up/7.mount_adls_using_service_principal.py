# Databricks notebook source
# MAGIC %md
# MAGIC #Access Azure data lake using Access Keys
# MAGIC
# MAGIC
# MAGIC #steps to follow
# MAGIC - Get CLient_id, tenant_id and client_secret from key vault
# MAGIC - set spark config with CLient_id, tenant_id and client_secret
# MAGIC - call file system utility mount to mount the storage
# MAGIC - Explore other file system utilities related to mount (lit a mount, unmount)
# MAGIC

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/formula1")

# COMMAND ----------

dbutils.secrets.listScopes()
dbutils.secrets.list("formula1-group")

# COMMAND ----------

access_key = dbutils.secrets.get("formula1-group", key="Formula1-access-key")

storage_account_name = "formula1surabhi"

container_name = "raw"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key)


# COMMAND ----------


dbutils.fs.mount(
    source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point="/mnt/formula1",
    extra_configs={f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key}
)

# COMMAND ----------


dbutils.fs.mount(
    source=f"wasbs://processed@{storage_account_name}.blob.core.windows.net",
    mount_point="/mnt/formula1/processed",
    extra_configs={f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key}
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1surabhi.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1surabhi.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("formula1-group")

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())