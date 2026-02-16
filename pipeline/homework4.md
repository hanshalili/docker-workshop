DE-Homework-4


Question 1

Answer: stg_green_tripdata, stg_yellow_tripdata, and int_trips_unioned (upstream dependencies)

Question 2

Answer: dbt will fail the test, returning a non-zero exit code

Question 3

duckdb /workspaces/docker-workshop/pipeline/taxi_rides_ny/taxi_rides_ny.duckdb -c "
select count(*) as cnt
from dev.fct_monthly_zone_revenue;
"


Answer: 14,120

Question 4

duckdb /workspaces/docker-workshop/pipeline/taxi_rides_ny/taxi_rides_ny.duckdb -c "
select
  pickup_zone,
  sum(revenue_monthly_total_amount) as total_revenue
from dev.fct_monthly_zone_revenue
where service_type = 'green'
  and strftime(revenue_month, '%Y') = '2020'
group by 1
order by total_revenue desc
limit 1;
"




Answer: East Harlem South

Question 5

duckdb /workspaces/docker-workshop/pipeline/taxi_rides_ny/taxi_rides_ny.duckdb -c "
select
  sum(total_monthly_trips) as trips
from dev.fct_monthly_zone_revenue
where service_type = 'green'
  and revenue_month = date '2019-10-01';
"



Answer: 384,624

Question 6

duckdb /workspaces/docker-workshop/pipeline/taxi_rides_ny/taxi_rides_ny.duckdb -c "
select count(*) as cnt
from dev.stg_fhv_tripdata;
"


Answer: 43,244,693