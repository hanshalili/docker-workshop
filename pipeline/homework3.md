DE-Homework-2

Question 1

SELECT COUNT(*)
FROM `dtc-kestra-project.zoomcamp_module_3.yellow_2024_external`;

Answer: 20,332,093.


Question 2

SELECT COUNT(DISTINCT PULocationID)
FROM `dtc-kestra-project.zoomcamp_module_3.yellow_2024`;

SELECT COUNT(DISTINCT PULocationID)
FROM `dtc-kestra-project.zoomcamp_module_3.yellow_2024_external`;

Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table


Question 3

SELECT PULocationID
FROM `dtc-kestra-project.zoomcamp_module_3.yellow_2024`;

SELECT PULocationID, DOLocationID
FROM `dtc-kestra-project.zoomcamp_module_3.yellow_2024`;

Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.


Question 4

SELECT count(*)
FROM `dtc-kestra-project.zoomcamp_module_3.yellow_2024`
WHERE fare_amount = 0;

Answer: 8333


Question 5

CREATE OR REPLACE TABLE `dtc-kestra-project.zoomcamp_module_3.yellow_2024_part_clust`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `dtc-kestra-project.zoomcamp_module_3.yellow_2024`;

Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID


Question 6

SELECT DISTINCT VendorID
FROM `dtc-kestra-project.zoomcamp_module_3.yellow_2024`
WHERE tpep_dropoff_datetime >= '2024-03-01'
  AND tpep_dropoff_datetime <  '2024-03-16';

SELECT DISTINCT VendorID
FROM `dtc-kestra-project.zoomcamp_module_3.yellow_2024_part_clust`
WHERE tpep_dropoff_datetime >= '2024-03-01'
  AND tpep_dropoff_datetime <  '2024-03-16';

Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table


Question 7

Answer: GCP Bucket


Question 8

Answer: False


Question 9

SELECT COUNT(*)
FROM `dtc-kestra-project.zoomcamp_module_3.yellow_2024`;

Answer: Because there’s no filter, join, or column computation, BigQuery may not need to scan any data blocks, so the job reports 0 bytes processed.