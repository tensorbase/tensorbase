-- nyc_taxi create table sample ddl
-- just favor several columns for demo: trip_id: 0; pickup_datetime: 2;passenger_count: 10
CREATE TABLE nyc_taxi (
    trip_id INT32 PRIMARY KEY, 
    pickup_datetime UNIX_DATETIME NOT NULL,
    passenger_count UINT8 NOT NULL
)