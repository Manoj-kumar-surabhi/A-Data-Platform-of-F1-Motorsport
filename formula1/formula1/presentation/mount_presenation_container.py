# Databricks notebook source

storage_account_name = "formula1surabhi"
container_name = "presentation"
access_key = "vVDkoXHxryRWJj5/ttts404XFhw0cQautIPkjmo3/qlOv63qW5k++XkqkEegM+oauNwzQYWUsgT1+AStXNAwcg=="

# Set the configuration for the storage account access
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", access_key)


# COMMAND ----------

mount_point = f"/mnt/formula1/{container_name}"

dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point = mount_point,
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key}
)


# COMMAND ----------

storage_account_name = "formula1surabhi"
container_name = "demo"
access_key = "vVDkoXHxryRWJj5/ttts404XFhw0cQautIPkjmo3/qlOv63qW5k++XkqkEegM+oauNwzQYWUsgT1+AStXNAwcg=="

# Set the configuration for the storage account access
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", access_key)

# COMMAND ----------

mount_point = f"/mnt/formula1/{container_name}"
dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point = mount_point,
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key}
)
