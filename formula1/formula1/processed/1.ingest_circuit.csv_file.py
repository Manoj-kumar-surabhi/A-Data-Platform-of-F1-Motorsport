# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest circuit.csv file

# COMMAND ----------

# MAGIC %md 
# MAGIC ###step-1 Read the csv file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("data_source", "")

v_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields = [
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("country", StringType(),True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read .option("header", "true").schema(circuits_schema).csv(f"{raw_data_path}/{v_file_date}//circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Selecting the columns we need
# MAGIC # this type below have the ability to use functions on the table

# COMMAND ----------

from pyspark.sql.functions import col 

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3 Rename the Columns  

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref").withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #step 4 Add a column
# MAGIC

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # write to data lake storage

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("hive_metastore.f1_processed.circuits")

# COMMAND ----------

display(
    spark.read.table("hive_metastore.f1_processed.circuits")
)

# COMMAND ----------

dbutils.notebook.exit("Success")