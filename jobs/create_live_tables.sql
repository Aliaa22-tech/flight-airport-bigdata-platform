CREATE TABLE IF NOT EXISTS flights.flights_live
(
    event_time DateTime,
    flight_date Date,
    airline LowCardinality(String),
    origin LowCardinality(String),
    dest LowCardinality(String),
    route String,
    dep_time_blk LowCardinality(String),
    dep_delay Nullable(Float32),
    arr_delay Nullable(Float32),
    cancelled UInt8,
    cancellation_code LowCardinality(String),
    distance Nullable(Float32),
    delay_category LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY event_time
TTL event_time + INTERVAL 1 DAY;

CREATE TABLE IF NOT EXISTS flights.live_airline_stats
(
    batch_time DateTime,
    airline LowCardinality(String),
    flights UInt32,
    delayed_flights UInt32,
    cancelled_flights UInt32,
    total_dep_delay Float64
)
ENGINE = MergeTree
ORDER BY (batch_time, airline)
TTL batch_time + INTERVAL 1 DAY;