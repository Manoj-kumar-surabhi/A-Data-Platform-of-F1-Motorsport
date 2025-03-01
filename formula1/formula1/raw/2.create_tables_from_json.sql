-- Databricks notebook source
-- MAGIC %md
-- MAGIC create table from constructors.json

-- COMMAND ----------

create table if not exists f1_raw.constructors_raw
(
  constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING
)
using json
options(path "/mnt/formula1/constructors.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC create tables from drivers.json

-- COMMAND ----------


create table if not exists f1_raw.drivers_raw
(
driverId int,  driverRef string, code string, dob date, name struct<forename string, surname string>, nationality string, number string
)
using json
options (path "/mnt/formula1/drivers.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC create table from results.json

-- COMMAND ----------

show tables in f1_raw;


-- COMMAND ----------

create table if not exists f1_raw.results_raw
(
  constructorId INT, 
  driverId INT, 
  fastestLap INT, 
  fastestLapSpeed FLOAT, 
  fastestLapTime STRING, 
  grid INT, 
  laps INT, 
  milliseconds INT, 
  number INT, 
  points FLOAT, 
  position INT, 
  positionOrder INT, 
  positionText INT, 
  rank INT, 
  raceId INT, 
  resultId INT, 
  statusId STRING, 
  time STRING
)
using json
options(path "/mnt/formula1/results.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC create a table from pit_stops.json
-- MAGIC -- multiline file

-- COMMAND ----------

create table if not exists f1_raw.pitstops_raw
(
  raceId int, driverId int, stop int, lap int, time string, duration string, milliseconds int
)
using json
options(path "/mnt/formula1/pit_stops.json", multiLine True)