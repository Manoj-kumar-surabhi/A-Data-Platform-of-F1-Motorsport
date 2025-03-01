-- Databricks notebook source
show databases;

-- COMMAND ----------

use f1_processed;

-- COMMAND ----------

show tables in f1_processed;

-- COMMAND ----------

SELECT * FROM f1_processed.drivers 
where (nationality = "British" AND dob <= '2001-08-20') or nationality="Indian"
ORDER BY nationality desc;

-- COMMAND ----------

