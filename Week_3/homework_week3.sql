-- Question 1--

SELECT
   COUNT(*)

FROM `terraform-demo-411906.ny_taxi.green_taxi_data`
;
-- Question 3 --

SELECT
   COUNT(*)

FROM `terraform-demo-411906.ny_taxi.green_taxi_data`
WHERE
   TRUE
   AND fare_amount = 0
;
-- Question 5 --

CREATE OR REPLACE TABLE `ny_taxi.green_taxi_data_partition` 
PARTITION BY DATE(lpep_pickup_datetime) AS
SELECT *
FROM `ny_taxi.green_taxi_data`
;
