-- Question 1--

SELECT
   COUNT(*)

FROM `terraform-demo-411906.ny_taxi.green_taxi_data`
;
-- Question 2 --
--Materialzed Table --
SELECT
   COUNT(DISTINCT PULocationID)

FROM `terraform-demo-411906.ny_taxi.green_taxi_data`
;
--External Table --
SELECT
   COUNT(DISTINCT PULocationID)

FROM `terraform-demo-411906.ny_taxi.green_taxi_ext_table`
;

-- Question 3 --

SELECT
   COUNT(*)

FROM `terraform-demo-411906.ny_taxi.green_taxi_data`
WHERE
   TRUE
   AND fare_amount = 0
;

-- Question 4 --

CREATE OR REPLACE TABLE `ny_taxi.green_taxi_data_partition` 
PARTITION BY DATE(lpep_pickup_datetime) AS
SELECT *
FROM `ny_taxi.green_taxi_data`
;

-- Question 5 --
SELECT
      COUNT(DISTINCT PULocationID)
FROM `ny_taxi.green_taxi_data_partition`
WHERE
   lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30'
;
