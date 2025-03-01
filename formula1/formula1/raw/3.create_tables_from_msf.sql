-- Databricks notebook source
create table if not exists f1_raw.lap_times_raw
(
  raceId int, driverId int, lap int, position int, time string, milliseconds int
)
using json 
options(path "/mnt/formula1/lap_times")

-- COMMAND ----------

create table if not exists f1_raw.qualifying_raw
(
  qualifyId int, raceId int, driverId int, constructorId int, number int, position int, q1 string, q2 string, q3 string
)
using json
options(path "/mnt/formula1/qualifying")