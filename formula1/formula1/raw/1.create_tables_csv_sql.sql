-- Databricks notebook source
create database f1_raw



-- COMMAND ----------

drop table if exists f1_raw.circuits_raw;
create table if not exists f1_raw.circuits_raw
(circuitId int,
circuitRef string,
name string,
location string,
country string,
lat double,
long double,
alt int,
url int
)
using csv
options(path "/mnt/formula1/circuits.csv", header "true")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC create races table

-- COMMAND ----------

create table if not exists f1_raw.races_raw
(
  raceId int,
  year int,
  round int,
  circuitId int,
  name string,
  date date,
  time string
)
using csv
options(path "/mnt/formula1/races.csv")