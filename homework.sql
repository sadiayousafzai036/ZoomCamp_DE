select count(*) from yellow_taxi_trips where extract(MONTH from tpep_pickup_datetime) = 01 and extract(day from tpep_pickup_datetime)=15;

select tpep_pickup_datetime from yellow_taxi_trips order by tip_amount desc limit 1;

select "Zone" from zones z inner join (select "DOLocationID",count(*) from zones z inner join yellow_taxi_trips y on z."LocationID"=y."PULocationID" where z."Zone"='Central Park' and extract(MONTH from "tpep_pickup_datetime")=01 and extract(day from "tpep_pickup_datetime")=14 group by 1 order by 2 desc limit 1)a on z."LocationID"=a."DOLocationID";

select concat(coalesce(z1."Zone",'Unknown'),'/',coalesce(z2."Zone",'Unknown')) from zones z1 inner join (select "PULocationID","DOLocationID",avg("total_amount") from yellow_taxi_trips group by "PULocationID","DOLocationID" order by 3 desc limit 1)a on z1."LocationID"=a."PULocationID" inner join zones z2 on z2."LocationID"=a."DOLocationID";