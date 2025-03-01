-- Databricks notebook source
select 
  driver_name,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2001 and 2010
group by driver_name
having count(1) >= 50
order by avg_points desc