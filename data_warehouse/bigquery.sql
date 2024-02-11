# Pre-questions work
CREATE OR REPLACE EXTERNAL TABLE `massive-oasis-412719.demo_dataset.external_green_taxi_2022`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage_green_taxi/green_taxi_2022/green_tripdata_2022-*.parquet']
);


CREATE OR REPLACE TABLE `massive-oasis-412719.demo_dataset.bq_green_taxi_2022` AS
SELECT * FROM `massive-oasis-412719.demo_dataset.external_green_taxi_2022`;


# Question 1
SELECT COUNT(*)
FROM `massive-oasis-412719.demo_dataset.external_green_taxi_2022`;

# Question 2
SELECT COUNT(DISTINCT PULocationID)
FROM `massive-oasis-412719.demo_dataset.external_green_taxi_2022`;

# Question 2
SELECT COUNT(DISTINCT PULocationID)
FROM `massive-oasis-412719.demo_dataset.bq_green_taxi_2022`;

# Question 3
SELECT COUNT(*)
FROM `massive-oasis-412719.demo_dataset.bq_green_taxi_2022`
WHERE fare_amount = 0
;

# Question 4
CREATE OR REPLACE TABLE `massive-oasis-412719.demo_dataset.bq_green_taxi_2022_partitoned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM `massive-oasis-412719.demo_dataset.external_green_taxi_2022`;

# Question 5
/* 
  Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
  Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

*/

## This query will process 12.82 MB
SELECT COUNT(DISTINCT PULocationID)
FROM `massive-oasis-412719.demo_dataset.bq_green_taxi_2022`
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30'
;


## This query will process 1.12 MB
SELECT COUNT(DISTINCT PULocationID)
FROM `massive-oasis-412719.demo_dataset.bq_green_taxi_2022_partitoned_clustered`
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30'



