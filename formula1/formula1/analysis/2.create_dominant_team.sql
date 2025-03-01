-- Databricks notebook source
select team_name,
count(1) as total_races,
sum(calculated_points) as total_points_of_team,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by team_name
having count(1) >= 100
order by avg_points desc

-- COMMAND ----------

describe table f1_presentation.calculated_race_results;