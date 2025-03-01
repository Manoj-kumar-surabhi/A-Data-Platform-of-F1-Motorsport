# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

display(spark.read.csv("dbfs:/mnt/formula1/mental_health").printSchema())

# COMMAND ----------

mental_data_df = spark.read.csv("dbfs:/mnt/formula1/mental_health", header=True, inferSchema=True)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Initialize Spark session
#spark = SparkSession.builder \
    #.appName("Mental Health Data Preprocessing") \
    #.getOrCreate()

# Load the CSV file
#df = spark.read.csv("/opt/spark/mental_health_dataset.csv", header=True, inferSchema=True)

# Show the initial dataframe structure
display(df)

# Convert columns to appropriate data types
health_schema = StructType(fields = [
    StructField("Timestamp", StringType(), False),
    StructField("Gender", StringType(), True),
    StructField("Occupation", StringType(), True),
    StructField("self_employed", StringType(), True),
    StructField("family_history", StringType(),True),
    StructField("treatment", StringType(), True),
    StructField("Days_Indoors", StringType(), True),
    StructField("Growing_Stress", StringType(), True),
    StructField("Changes_Habits", StringType(), True),
     StructField("Mental_Health_History", StringType(), True),
    StructField("Mood_Swings", StringType(), True),
    StructField("Coping_Struggles", StringType(),True),
    StructField("Work_Interest", StringType(), True),
    StructField("Social_Weakness", StringType(), True),
    StructField("mental_health_interview", StringType(), True),
    StructField("care_options", StringType(), True)
])

df = spark.read .option("header", "true").schema(health_schema).csv("dbfs:/mnt/formula1/mental_health")

# COMMAND ----------


# Handle missing values
df = df.na.fill("Unknown")

# Encode categorical variables
categorical_cols = [col for col in df.columns if df.select(col).dtypes[0][1] == 'string']

indexers = [StringIndexer(inputCol=col, outputCol=col+"_index", handleInvalid="keep") for col in categorical_cols]
encoders = [OneHotEncoder(inputCol=col+"_index", outputCol=col+"_vec") for col in categorical_cols]

# Assemble features
assembler = VectorAssembler(inputCols=[col+"_vec" for col in categorical_cols] , outputCol="features")

# Create and fit the pipeline
pipeline = Pipeline(stages=indexers + encoders + [assembler])
model = pipeline.fit(df)
df_encoded = model.transform(df)

# Select relevant columns
df_final = df_encoded.select("features", "treatment")

display(df_final)

# COMMAND ----------

# Show the preprocessed dataframe
#
# df_final.show(5)


# Generate some basic insights
# 1. Distribution of treatment
#df.groupBy("treatment").count().show()


# 3. Count of employees who have a family history of mental illness and sought treatment
#df.groupBy("treatment", "family_history").count().show()

# Save preprocessed data
#df_final.write.parquet("/opt/spark/preprocessed_mental_health_data.parquet")

# Stop the Spark session
#spark.stop()