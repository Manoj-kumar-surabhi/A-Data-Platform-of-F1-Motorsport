-- Databricks notebook source
use f1_processed;


-- COMMAND ----------

select max(dob), nationality from drivers

-- COMMAND ----------

select nationality, count(*)
from drivers
group by nationality having count(*) > 50
order by nationality;

-- COMMAND ----------

select nationality, name, dob, RANK() over(partition by nationality order by dob desc) as age_rank
from drivers
order by nationality, age_rank