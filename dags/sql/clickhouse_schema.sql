CREATE TABLE IF NOT EXISTS dim_community_area
(
    community_area_id UInt8,
    community_area_name String
)
ENGINE = MergeTree
ORDER BY community_area_id;

CREATE TABLE IF NOT EXISTS dim_date
(
    date Date,
    year UInt16,
    month UInt8,
    day UInt8,
    day_name LowCardinality(String),
    is_weekend Bool
)
ENGINE = MergeTree
ORDER BY date;

CREATE TABLE IF NOT EXISTS dim_payment_type
(
    payment_type_id UInt16,
    payment_type String,
    tips_recorded Bool
)
ENGINE = MergeTree
ORDER BY payment_type_id;

CREATE TABLE IF NOT EXISTS dim_company
(
    company_id UInt16,
    company String,
    company_name String
)
ENGINE = MergeTree
ORDER BY company_id;

CREATE TABLE IF NOT EXISTS fact_trips
(
    trip_id String,
    taxi_id String,
    trip_date Date,
    trip_start_timestamp DateTime,
    trip_end_timestamp DateTime,
    pickup_community_area_id UInt8,
    dropoff_community_area_id UInt8,
    payment_type_id UInt16,
    company_id UInt16,
    trip_seconds Nullable(UInt32),
    trip_miles Nullable(Float64),
    fare Nullable(Float64),
    tips Nullable(Float64),
    tolls Nullable(Float64),
    extras Nullable(Float64),
    trip_total Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trip_date)
ORDER BY (trip_date, pickup_community_area_id);

CREATE OR REPLACE VIEW kpi_daily_revenue AS
SELECT
    trip_date,
    count() AS trips,
    round(sum(trip_total), 2) AS revenue,
    round(avgIf(trip_seconds, trip_seconds > 0) / 60, 1) AS avg_trip_minutes
FROM fact_trips
GROUP BY trip_date;

CREATE OR REPLACE VIEW kpi_pickup_zones AS
SELECT
    z.month,
    a.community_area_name AS pickup_zone,
    z.trips,
    z.revenue,
    z.avg_trip_minutes
FROM
(
    SELECT
        toStartOfMonth(trip_date) AS month,
        pickup_community_area_id,
        count() AS trips,
        round(sum(trip_total), 2) AS revenue,
        round(avgIf(trip_seconds, trip_seconds > 0) / 60, 1) AS avg_trip_minutes
    FROM fact_trips
    GROUP BY month, pickup_community_area_id
) AS z
INNER JOIN dim_community_area AS a ON z.pickup_community_area_id = a.community_area_id;
