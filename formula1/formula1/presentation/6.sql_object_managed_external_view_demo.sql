-- Databricks notebook source
-- MAGIC %md
-- MAGIC Learning Objectives
-- MAGIC 1. Create Managed table using Python
-- MAGIC 2. Create Managed table using sql
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe data

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Writing data file into database using Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_data_path}/race_results")
-- MAGIC race_results_df.withColumnRenamed("position", "driver_position")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.mode("overwrite").format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

describe extended race_results_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC write race results file to demo databse using sql

-- COMMAND ----------

Use demo;

-- COMMAND ----------

CREATE table if not exists race_results_sql
Using parquet
As
SELECT *
 From demo.race_results_python
 where grid <= 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC in the next example i have created a managed table directly from the adls container named presentation
-- MAGIC - in the above example, the table is created from demo database

-- COMMAND ----------

create table if not exists demo.constructor_results
using parquet
as 
select *
from parquet.`/mnt/formula1/presentation/constructor_standings`

-- COMMAND ----------

describe extended race_results_sql

-- COMMAND ----------

SELECT * FROM race_results_sql

-- COMMAND ----------

drop table if exists race_results_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Till now I created managed tables where user has control of dropping the table and hive metadata.
-- MAGIC
-- MAGIC Now I going to create external tables on which we access on Hive metadata

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.mode("overwrite").format("parquet").option("path", f"{presentation_data_path}/race_results_ext_py").saveAsTable("demo.race_results_df_python")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/mnt/formula1/presentation/race_results_ext_py/"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC to create an external table we need to specify the location of the container or adls

-- COMMAND ----------

create table if not exists demo.race_result_sql
using parquet 
location "/mnt/formula1/presentation/race_results_sql"

-- COMMAND ----------

insert into demo.race_result_sql
select 
    race_year,
    race_name,
    race_date,
    circuit_location,
    cast(grid as int) as grid,
    cast(fastestLap as int) as fastestLap,
    cast(points as float) as points,
    race_time,
    cast(position as int) as position,
    driver_name,
    driver_nationality,
    cast(driver_number as int) as driver_number,
    team,
    nationality,
    created_date
from demo.race_results_df_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Views
-- MAGIC Temporary view 
-- MAGIC
-- MAGIC

-- COMMAND ----------

create temp view  t_race_results
as 
select * 
from demo.race_results_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Global temporary view
-- MAGIC - once the cluster terminates, we cant acess the global temp views as it deletes the table
-- MAGIC

-- COMMAND ----------

create or replace global temp view g_constructor_results as
select * from  parquet.`/mnt/formula1/presentation/constructor_standings`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Permanant views
-- MAGIC - can acess the views from any notebook
-- MAGIC - Even if the cluster terminates they will stay in databases

-- COMMAND ----------

create or replace view demo.p_race_results as 
select * from demo.race_results_python

-- COMMAND ----------

