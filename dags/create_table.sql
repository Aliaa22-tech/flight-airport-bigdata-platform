CREATE TABLE IF NOT EXISTS flights.flights
(
    Year UInt16,
    Quarter UInt8,
    Month UInt8,
    DayOfMonth UInt8,
    DayOfWeek UInt8,
    FlightDate Date,
    Reporting_Airline LowCardinality(String),
    Origin LowCardinality(String),
    OriginCityName String,
    OriginState LowCardinality(String),
    Dest LowCardinality(String),
    DestCityName String,
    DestState LowCardinality(String),
    DepTimeBlk LowCardinality(String),
    DepDelay Nullable(Float32),
    DepDel15 Nullable(Float32),
    ArrDelay Nullable(Float32),
    ArrDel15 Nullable(Float32),
    Cancelled UInt8,
    CancellationCode LowCardinality(String),
    Diverted UInt8,
    Distance Nullable(Float32),
    CarrierDelay Nullable(Float32),
    WeatherDelay Nullable(Float32),
    NASDelay Nullable(Float32),
    SecurityDelay Nullable(Float32),
    LateAircraftDelay Nullable(Float32)
)
ENGINE = MergeTree
ORDER BY (FlightDate, Origin, Reporting_Airline)