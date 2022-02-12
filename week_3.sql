SELECT count(*) FROM `zoomcamp-de-339322.trips_data_all.green_tripdata_external_table` ;

SELECT count(*) FROM `zoomcamp-de-339322.trips_data_all.green_tripdata_external_table` ;


SELECT count(*) FROM `zoomcamp-de-339322.trips_data_all.green_tripdata_PC`
where   date(dropoff_datetime)  >= '2019-01-01' and  date(dropoff_datetime)<'2019-03-31'
and  dispatching_base_num in ('B00987', 'B02060', 'B02279') ;



CREATE OR REPLACE TABLE `zoomcamp-de-339322.trips_data_all.green_tripdata_PC` 
            PARTITION BY DATE(dropoff_datetime) 
            CLUSTER BY dispatching_base_num  AS   SELECT * FROM `zoomcamp-de-339322.trips_data_all.green_tripdata_external_table` 