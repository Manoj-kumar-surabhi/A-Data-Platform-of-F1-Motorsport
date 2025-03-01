-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

create table if not exists f1_presentation.calculated_race_results
using parquet
as
select races.race_year,
constructors.name as team_name,
drivers.name as driver_name,
results.position,
results.points,
11 - results.position as calculated_points

from results
join drivers on (results.driverId = drivers.driver_id)
join races on (results.raceId = races.race_id)
join constructors on (results.constructorId = constructors.constructor_id)

where results.position <= 10 

-- COMMAND ----------

select * from f1_presentation.calculated_race_results