-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

describe database demo

-- COMMAND ----------

-- MAGIC %r
-- MAGIC # Load the SparkR package
-- MAGIC library(SparkR)
-- MAGIC
-- MAGIC # Read the CSV file into a Spark DataFrame
-- MAGIC df <- read.df(
-- MAGIC   path = "/mnt/formula1/circuits.csv",
-- MAGIC   source = "csv",
-- MAGIC   header = "true",
-- MAGIC   inferSchema = "true",
-- MAGIC   delimiter = ","
-- MAGIC )
-- MAGIC
-- MAGIC # Display the DataFrame
-- MAGIC display(df)