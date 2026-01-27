For Homework Module 1 tasks.
---------------------------------------------------------------------
#3. 
Changed ingest_data.py to ingest_data_homework.py, ran docker compose 
SQL code for task:
SELECT COUNT(*) AS trips_count
FROM green_tripdata_2025_11
WHERE
    lpep_pickup_datetime >= '2025-11-01'
    AND lpep_pickup_datetime < '2025-12-01'
    AND trip_distance <= 1;
---------------------------------------------------------------------
#4.
SQL code for task:
SELECT
    DATE(lpep_pickup_datetime) AS pickup_date,
    MAX(trip_distance) AS max_trip_distance
FROM green_tripdata_2025_11
WHERE trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_trip_distance DESC
LIMIT 1;
---------------------------------------------------------------------
#5.
SQL code for task:
SELECT
    z."Zone" AS pickup_zone,
    SUM(t.total_amount) AS total_amount_sum
FROM green_tripdata_2025_11 t
JOIN taxi_zone_lookup z
    ON t."PULocationID" = z."LocationID"
WHERE
    t.lpep_pickup_datetime >= '2025-11-18'
    AND t.lpep_pickup_datetime < '2025-11-19'
GROUP BY z."Zone"
ORDER BY total_amount_sum DESC
LIMIT 1;
---------------------------------------------------------------------
#6.
SQL code for task:
SELECT
    t.tip_amount,
    z_do."Zone" AS dropoff_zone,
    t.lpep_pickup_datetime,
    t.lpep_dropoff_datetime
FROM green_tripdata_2025_11 t
JOIN taxi_zone_lookup z_pu
    ON t."PULocationID" = z_pu."LocationID"
JOIN taxi_zone_lookup z_do
    ON t."DOLocationID" = z_do."LocationID"
WHERE
    z_pu."Zone" = 'East Harlem North'
    AND t.lpep_pickup_datetime >= '2025-11-01'
    AND t.lpep_pickup_datetime < '2025-12-01'
ORDER BY t.tip_amount DESC
LIMIT 1;
---------------------------------------------------------------------


