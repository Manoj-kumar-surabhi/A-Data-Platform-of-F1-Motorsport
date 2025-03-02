-- Databricks notebook source
create or replace temp view v_driver_ranks
as
select 
  driver_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points,
  RANK() over(order by avg(calculated_points) desc) as driver_rank
from f1_presentation.calculated_race_results
 group by driver_name
having count(1) >= 50
order by avg_points desc

-- COMMAND ----------

 
select race_year,
  driver_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points
  from f1_presentation.calculated_race_results
  where driver_name in (select driver_name from v_driver_ranks)
 group by race_year,driver_name
order by race_year, avg_points desc