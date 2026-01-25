

docker compose up


docker network ls


docker run -it --rm\
  --network=pipeline_default \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-table=yellow_taxi_trips



DE-Homework-1
Question 1
docker run -it --rm --entrypoint bash python:3.13
pip -V
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)

Answer: 25.3


Question 2 

pgAdmin runs in the same Docker Compose network as Postgres, so it must use the service name (db) and the container port (5432), not localhost or the mapped host port.

Answer: db:5432





wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv


Question 3 

SELECT COUNT(1)
FROM green_taxi_data
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime <  '2025-12-01'
  AND trip_distance <= 1;

Answer: 8007


Question 4 

SELECT lpep_pickup_datetime::DATE AS pickup_day, MAX(trip_distance) AS max_trip_distance
FROM green_taxi_data
WHERE trip_distance < 100
GROUP BY pickup_day
ORDER BY max_trip_distance DESC
LIMIT 1;

Answer: 2025-11-14


Question 5 

SELECT z."Zone" AS pickup_zone, SUM(t.total_amount) AS total_amount_sum
FROM green_taxi_data t
JOIN taxi_zones z
  ON t."PULocationID" = z."LocationID"
WHERE t.lpep_pickup_datetime >= '2025-11-18'
  AND t.lpep_pickup_datetime <  '2025-11-19'
GROUP BY 1
ORDER BY total_amount_sum DESC
LIMIT 1;

Answer: East Harlem North


Question 6 

SELECT z_do."Zone" AS dropoff_zone, MAX(t.tip_amount) AS max_tip
FROM green_taxi_data t
JOIN taxi_zones z_pu
  ON t."PULocationID" = z_pu."LocationID"
JOIN taxi_zones z_do
  ON t."DOLocationID" = z_do."LocationID"
WHERE t.lpep_pickup_datetime >= '2025-11-01'
  AND t.lpep_pickup_datetime <  '2025-12-01'
  AND z_pu."Zone" = 'East Harlem North'
GROUP BY 1
ORDER BY max_tip DESC
LIMIT 1;

Answer: Yorkville West


Question 7 

Answer: terraform init, terraform apply -auto-approve, terraform destroy