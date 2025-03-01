# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1/demo'

# COMMAND ----------

results_df = spark.read.option("inferSchema", True).json("/mnt/formula1/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_paritioned")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update and delete the data

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.results_managed
# MAGIC set points = 11 - position
# MAGIC where position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC Delete from f1_demo.results_managed
# MAGIC where position > 10

# COMMAND ----------

# MAGIC %md
# MAGIC #Merge or upsert to Delta lake

# COMMAND ----------

drivers_day_1_df = spark.read.option("inferSchema", True).json("/mnt/formula1/2021-03-28/drivers.json").filter("driverId <= 10").select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day_2_df = spark.read.option("inferSchema", True).json("/mnt/formula1/2021-03-28/drivers.json").filter("driverId between 6 and 15").select("driverId", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"),"dob")

# COMMAND ----------

drivers_day_3_df = spark.read.option("inferSchema", True).json("/mnt/formula1/2021-03-28/drivers.json").filter("driverId between 1 and 5 or driverId between 11 and 15").select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day_1_df.createOrReplaceTempView("drivers_day_1")

# COMMAND ----------

drivers_day_2_df.createOrReplaceTempView("drivers_day_2")

# COMMAND ----------

# MAGIC %md
# MAGIC ###lets create a table in f1 demo to demonstrate merge statements in delta lake

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists hive_metastore.f1_demo.drivers_merge(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createDate DATE,
# MAGIC   updatedDate Date
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %md
# MAGIC sql syntax for update or inserting records
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into f1_demo.drivers_merge as target
# MAGIC using drivers_day_1 as updated
# MAGIC on target.driverId = updated.driverId
# MAGIC when matched then 
# MAGIC update set target.dob = updated.dob,
# MAGIC            target.forename = updated.forename,
# MAGIC            target.surname = updated.surname,
# MAGIC            target.updatedDate = current_timestamp
# MAGIC when not matched then
# MAGIC          insert (driverId, dob, forename, surname, createDate) values (driverId, dob, forename, surname, current_timestamp)           

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into f1_demo.drivers_merge as target
# MAGIC using drivers_day_2 as updated
# MAGIC on target.driverId = updated.driverId
# MAGIC when matched then 
# MAGIC update set target.dob = updated.dob,
# MAGIC            target.forename = updated.forename,
# MAGIC            target.surname = updated.surname,
# MAGIC            target.updatedDate = current_timestamp
# MAGIC when not matched then
# MAGIC          insert (driverId, dob, forename, surname, createDate) values (driverId, dob, forename, surname, current_timestamp)       

# COMMAND ----------

# MAGIC %md
# MAGIC pyspark syntax for updating or inserting data
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1/demo/drivers_merge")

deltaTable.alias("tgt").merge(
  drivers_day_3_df.alias("upd"),
  "tgt.driverId = upd.driverId"
) \
.whenMatchedUpdate(
  set = {
    "dob": "upd.dob", 
    "forename": "upd.forename", 
    "surname": "upd.surname", 
    "updatedDate": "current_timestamp()"
  }
) \
.whenNotMatchedInsert(
  values = {
    "dob": "upd.dob", 
    "forename": "upd.forename", 
    "surname": "upd.surname", 
    "createDate": "current_timestamp()"
  }
) \
.execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY hive_metastore.f1_demo.drivers_merge

# COMMAND ----------

dbutils.fs.ls("/")