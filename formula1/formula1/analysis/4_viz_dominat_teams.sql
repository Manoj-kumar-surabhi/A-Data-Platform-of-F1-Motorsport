-- Databricks notebook source
create or replace temp view v_dominant_teams
as
select team_name,
count(1) as total_races,
sum(calculated_points) as total_points_of_team,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by team_name
having count(1) >= 100
order by avg_points desc

-- COMMAND ----------

describe table v_dominant_teams

-- COMMAND ----------

select team_name, race_year,
sum(calculated_points) as total_points_of_team,
avg(calculated_points) as avg_points_by_year
from f1_presentation.calculated_race_results
where team_name in (select team_name from v_dominant_teams)
group by race_year, team_name
order by race_year, avg_points_by_year desc