--Total Fair amount for each vendor
SELECT VendorID,sum(fare_amount)as total_fare FROM `innate-paratext-394119.uber_data_engineering.fact_table` 
group by VendorID;

--Find the average tip amount made by different payment types in descending order
SELECT b.payment_type_name, avg(a.tip_amount) as Avg_tip_amount FROM `innate-paratext-394119.uber_data_engineering.fact_table` a
join innate-paratext-394119.uber_data_engineering.payment_type_dim b
on a.payment_type_id=b.payment_type_id
group by b.payment_type_name
order by Avg_tip_amount desc;

--top 10 pickup locs based on num of trips
SELECT pickup_location_id, COUNT(*) AS trip_count
FROM `innate-paratext-394119.uber_data_engineering.fact_table` 
GROUP BY pickup_location_id
ORDER BY trip_count DESC
LIMIT 10;

--total no of trips by passesnger count
SELECT c.passenger_count, COUNT(a.trip_id) AS trip_count
FROM `innate-paratext-394119.uber_data_engineering.fact_table` a
join innate-paratext-394119.uber_data_engineering.passenger_count_dim c 
on a.passenger_count_id=c.passenger_count_id
GROUP BY c.passenger_count
ORDER BY trip_count DESC;

--Avg fare amt by hour of the day
SELECT d.pick_hour, avg(a.fare_amount) as avg_fare_amt
FROM `innate-paratext-394119.uber_data_engineering.fact_table` a
join innate-paratext-394119.uber_data_engineering.datetime_dim d 
on a.datetime_id=d.datetime_id
GROUP BY d.pick_hour
ORDER BY avg_fare_amt DESC;

--Creating a table "tbl_analytics" combining the required columns in order to load the data on to Looker for further Analysis by developing Visualizations
CREATE OR REPLACE TABLE `innate-paratext-394119.uber_data_engineering.tbl_analytics` AS (
SELECT 
f.trip_id,
f.VendorID,
d.tpep_pickup_datetime,
d.tpep_dropoff_datetime,
p.passenger_count,
t.trip_distance,
r.rate_code_name,
pick.pickup_latitude,
pick.pickup_longitude,
drop.dropoff_latitude,
drop.dropoff_longitude,
pay.payment_type_name,
f.fare_amount,
f.extra,
f.mta_tax,
f.tip_amount,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount
FROM 
`innate-paratext-394119.uber_data_engineering.fact_table` f
JOIN `innate-paratext-394119.uber_data_engineering.datetime_dim` d  ON f.datetime_id=d.datetime_id
JOIN `innate-paratext-394119.uber_data_engineering.passenger_count_dim` p  ON p.passenger_count_id=f.passenger_count_id  
JOIN `innate-paratext-394119.uber_data_engineering.trip_distance_dim` t  ON t.trip_distance_id=f.trip_distance_id  
JOIN `innate-paratext-394119.uber_data_engineering.rate_code_dim` r ON r.rate_code_id=f.rate_code_id  
JOIN `innate-paratext-394119.uber_data_engineering.pickup_location_dim` pick ON pick.pickup_location_id=f.pickup_location_id
JOIN `innate-paratext-394119.uber_data_engineering.dropoff_location_dim` drop ON drop.dropoff_location_id=f.dropoff_location_id
JOIN `innate-paratext-394119.uber_data_engineering.payment_type_dim` pay ON pay.payment_type_id=f.payment_type_id)
;
