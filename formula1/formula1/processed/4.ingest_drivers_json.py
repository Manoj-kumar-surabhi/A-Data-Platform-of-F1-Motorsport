# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 import drivers.json file and create a schema 

# COMMAND ----------

dbutils.widgets.text("data_source", "")

v_source_data_4 = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType([StructField("forename", StringType()), StructField("surname", StringType())])

# COMMAND ----------

drivers_schema = StructType([
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("code", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("name", name_schema),
    StructField("nationality", StringType(), True),
    StructField("number", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{raw_data_path}/{v_file_date}/drivers.json")



# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 Remove the url column

# COMMAND ----------

drivers_dropped_df = drivers_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import  concat, col, lit

# COMMAND ----------

drivers_renamed_df = drivers_dropped_df.withColumnRenamed("driverId", "driver_id") \
                                       .withColumnRenamed("driverRef", "driver_ref") \
                                       .withColumn("name", concat(col("name.forename"), lit(' '), col("name.surname"))) 


drivers_renamed_df = add_ingestion_date(drivers_renamed_df).withColumn("data_source", lit(v_source_data_4)).withColumn("file_date", lit(v_file_date))
                               

# COMMAND ----------

drivers_renamed_df.write.mode("overwrite").format("delta").saveAsTable("hive_metastore.f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")