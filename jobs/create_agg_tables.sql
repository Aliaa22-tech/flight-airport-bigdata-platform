CREATE TABLE IF NOT EXISTS flights.agg_airline
(
    airline String,
    total_flights Int64,
    cancelled Int64,
    delayed Int64,
    avg_dep_delay Nullable(Float64),
    avg_arr_delay Nullable(Float64),
    delay_rate Nullable(Float64),
    cancellation_rate Nullable(Float64)
)
ENGINE = MergeTree ORDER BY airline;

CREATE TABLE IF NOT EXISTS flights.agg_airport
(
    airport String,
    city String,
    state String,
    total_flights Int64,
    cancelled Int64,
    delayed Int64,
    avg_dep_delay Nullable(Float64),
    avg_arr_delay Nullable(Float64),
    delay_rate Nullable(Float64),
    cancellation_rate Nullable(Float64)
)
ENGINE = MergeTree ORDER BY airport;

CREATE TABLE IF NOT EXISTS flights.agg_hour
(
    dep_hour Int32,
    total_flights Int64,
    cancelled Int64,
    delayed Int64,
    avg_dep_delay Nullable(Float64),
    avg_arr_delay Nullable(Float64),
    delay_rate Nullable(Float64),
    cancellation_rate Nullable(Float64)
)
ENGINE = MergeTree ORDER BY dep_hour;

CREATE TABLE IF NOT EXISTS flights.agg_month
(
    year Int32,
    month Int32,
    season String,
    total_flights Int64,
    cancelled Int64,
    delayed Int64,
    avg_dep_delay Nullable(Float64),
    avg_arr_delay Nullable(Float64),
    delay_rate Nullable(Float64),
    cancellation_rate Nullable(Float64)
)
ENGINE = MergeTree ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS flights.agg_dayofweek
(
    day_of_week Int32,
    total_flights Int64,
    cancelled Int64,
    delayed Int64,
    avg_dep_delay Nullable(Float64),
    avg_arr_delay Nullable(Float64),
    delay_rate Nullable(Float64),
    cancellation_rate Nullable(Float64)
)
ENGINE = MergeTree ORDER BY day_of_week;

CREATE TABLE IF NOT EXISTS flights.agg_route
(
    route String,
    origin String,
    dest String,
    total_flights Int64,
    cancelled Int64,
    delayed Int64,
    avg_dep_delay Nullable(Float64),
    avg_arr_delay Nullable(Float64),
    delay_rate Nullable(Float64),
    cancellation_rate Nullable(Float64)
)
ENGINE = MergeTree ORDER BY route;

CREATE TABLE IF NOT EXISTS flights.agg_delay_cause
(
    cause String,
    total_delay_minutes Float64,
    pct Float64
)
ENGINE = MergeTree ORDER BY cause;