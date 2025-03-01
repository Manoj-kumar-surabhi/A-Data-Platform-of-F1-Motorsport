-- Databricks notebook source

drop database if exists hive_metastore.f1_processed cascade;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/mnt/formula1/processed'))
-- MAGIC

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS hive_metastore.f1_processed
LOCATION "/mnt/formula1/processed"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/mnt/formula1/"))

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
 LOCATION "/mnt/formula1/presentation"

-- COMMAND ----------

drop database if exists hive_metastore.f1_presentation cascade

-- COMMAND ----------

create database if not exists hive_metastore.f1_presentation
location "/mnt/formula1/presentation"