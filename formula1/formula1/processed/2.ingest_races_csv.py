# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 Access data 

# COMMAND ----------

dbutils.widgets.text("data_source", "")

v_data_source_2 = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, IntegerType, StructField, DateType, TimestampType

# COMMAND ----------

races_schema = StructType(fields=[
  StructField("raceId", IntegerType(), False),
  StructField("year", IntegerType(), True),
  StructField("round", IntegerType(), True),
  StructField("circuitId", IntegerType(), True),
  StructField("name", StringType(), True),
  StructField("date", DateType(), True ),
  StructField ("time", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.option("header", "true").schema(races_schema).csv(f"{raw_data_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 : Selecting the columnns we need 

# COMMAND ----------

from pyspark.sql.functions import col, lit


# COMMAND ----------

races_selected_column = races_df.select(col("raceId"), col("year"), col("round"),col("circuitId"),col("name"),col("date"), col("time") )

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 Rename the columns

# COMMAND ----------

races_renamed_columns = races_selected_column.withColumnRenamed("raceId", "race_id").withColumnRenamed("year", "race_year").withColumnRenamed("circuitId", "circuit_id").withColumn("daa_source", lit(v_data_source_2)).withColumnRenamed("date", "race_date").withColumnRenamed("name", "race_name").withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC Step- 4 Concat the date and time to race_timestamp
# MAGIC

# COMMAND ----------

races_newcolumn_df = add_ingestion_date(races_renamed_columns)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit

# COMMAND ----------

races_concated_df = races_newcolumn_df.withColumn('race_timestamp',to_timestamp(concat(col('race_date'),lit(' '),col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_final_df = races_concated_df.select(
    col("race_id"), 
    col("race_year"), 
    col("round"), 
    col("circuit_id"), 
    col("race_name"), 
    col("race_date"),
    col("race_timestamp"), 
    col('ingestion_date')
)

# COMMAND ----------

# MAGIC %md
# MAGIC step 5 write the output to data lake storage
# MAGIC

# COMMAND ----------

races_final_df.write.mode('overwrite').format("delta").saveAsTable("hive_metastore.f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")