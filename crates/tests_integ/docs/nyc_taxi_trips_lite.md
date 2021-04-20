CREATE TABLE trips_lite
(
    trip_id UInt32,
    pickup_datetime DateTime
)
ENGINE = BaseStorage
PARTITION BY toYYYYMM(pickup_datetime)