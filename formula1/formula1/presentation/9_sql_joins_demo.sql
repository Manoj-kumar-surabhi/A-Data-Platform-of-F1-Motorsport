-- Databricks notebook source
use f1_presentation;

-- COMMAND ----------

create or replace temp view driver_standings_2001
as
select race_year, driver_name, driver_nationality, wins
from driver_standings
where race_year = 1988;

-- COMMAND ----------

create or replace temp view driver_standings_2002
as 
select race_year, driver_nationality, driver_name, wins
from driver_standings
where race_year = 1989

-- COMMAND ----------

select * from driver_standings_2001

-- COMMAND ----------

-- MAGIC %md
-- MAGIC inner join

-- COMMAND ----------

select *
from driver_standings_2001 as d_2001
left join driver_standings_2002 as d_2002
on (d_2001.driver_name = d_2002.driver_name)