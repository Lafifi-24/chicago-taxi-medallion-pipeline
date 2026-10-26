# Setup
## Requirements
- Docker
- 12.5 GB of RAM
- 22GB for Disk
## Running steps
- Make sure these ports are free:
    - 9000 and 9001 for MinIO
    - 19000 and 8123 for ClickHouse
    - 8080 for Airflow
- Run `docker compose up -d` to start the containers in the background.
- Open localhost:8080, enable all DAGs, and trigger the bronze one manually; the others will be triggered automatically.
# Plan
## Stack
- PySpark for data processing
- Docker for a simple setup
- Airflow for orchestration
- MinIO as the lake storage
- ClickHouse as a column-oriented database for analytics
## Strategy
The strategy uses four DAGs that follow the medallion architecture. The bronze ingestion DAG loads data from the city's open data API into the lake storage. In my test I focus on three years: 2021, 2022 and 2023 (to change the range, simply edit the dates in `bronze_ingestion.py`). The DAG has two levels of task groups, one for years and one for months, and each task downloads one day of that month into the bucket, following the structure `yyyy/mm/dd.csv`. Splitting the ingestion into small chunks makes error recovery cheaper: if a task fails, the pipeline does not restart a whole year, it only has to ingest that one day again. It also keeps memory use low, since each task holds only one day in memory before saving it to the lake storage. We end up with nearly 17 million rows (16,826,040).

The second DAG is the silver transformation, where I clean the data using PySpark. The first check validates each value's type against the types declared on the dataset's website. The second validates the values themselves, such as nulls or illogical values like a `trip_end_timestamp` earlier than the `trip_start_timestamp`. Invalid rows are saved in the quarantine folder, and the cleaned rows are saved in the silver bucket, in the `trips` folder, in Parquet format. This reduces the storage from 7.0 GB of CSV to 1.2 GB of Parquet. This DAG is triggered by every change in the bronze bucket.

The gold transformation DAG prepares the gold data and is triggered by the silver bucket. It builds the fact table and the dimension tables for date, company, community area and payment method. The results are saved in Parquet, with the fact table split by month.

The last DAG is the ClickHouse serving DAG. It is triggered by changes in the gold bucket, loads the gold data as tables, and creates two views that present the KPIs.
## Schema
The pipeline: only the bronze DAG is triggered manually, and each next DAG is triggered automatically when the previous layer changes.
```text
          Chicago open data API (Taxi Trips 2021-2023)
                             │  bronze_ingestion: manual trigger, one task per day
                             ▼
              ┌─────────────────────────────┐
              │   BRONZE (MinIO, bronze)    │  raw CSV, yyyy/mm/dd.csv
              └──────────────┬──────────────┘
                             │  silver_transformation (PySpark): types, rules, dedup
                             ▼
              ┌─────────────────────────────┐
              │   SILVER (MinIO, silver)    │  trips + quarantine, Parquet per day
              └──────────────┬──────────────┘
                             │  gold_transformation (PySpark): star schema
                             ▼
              ┌─────────────────────────────┐
              │   GOLD (MinIO, gold)        │  fact_trips per month + 4 dimensions
              └──────────────┬──────────────┘
                             │  clickhouse_serving: load tables + KPI views
                             ▼
              ┌─────────────────────────────┐
              │   CLICKHOUSE (analytics)    │  SQL for the BI team
              └─────────────────────────────┘
```
The star schema in gold and ClickHouse: the fact table holds only the keys and the measures, and the names live in the dimensions.
```text
                             ┌────────────────┐
                             │    dim_date    │
                             └───────┬────────┘
                                     │ trip_date
┌────────────────────┐       ┌───────┴────────┐       ┌──────────────────┐
│ dim_community_area ├───────┤   fact_trips   ├───────┤ dim_payment_type │
└────────────────────┘       └───────┬────────┘       └──────────────────┘
 pickup_community_area_id            │ company_id         payment_type_id
 dropoff_community_area_id           │
                             ┌───────┴────────┐
                             │  dim_company   │
                             └────────────────┘
```
# Decision explanations
- `BRONZE_WRITE_MODE` is `soft` by default, which means nothing already in the bronze bucket is overwritten: triggering the bronze DAG several times does not change existing data, it only adds the days that are missing. `hard` overwrites what already exists in the bronze bucket.
- I use Parquet for silver and gold, as it is optimized for typed data.
- I chose ClickHouse as it is column-based and optimized for data analytics.
- In this pipeline nothing is dropped: every row is classified in the right place, and invalid rows go to quarantine with the reason. The quarantine conditions and the number of rows for 2021-2023 are:
    - `unparseable_row`: the row cannot be parsed with the declared types (0 rows)
    - `missing_trip_id`: `trip_id` is empty (0 rows)
    - `missing_timestamp`: the start or end timestamp is empty (1,040 rows)
    - `end_before_start`: `trip_end_timestamp` is earlier than `trip_start_timestamp` (330 rows)
    - `missing_trip_total`: `trip_total` is empty (16,284 rows)
    - `negative_value`: a duration, distance or amount is negative (0 rows)
    - `invalid_community_area`: a community area is outside 1-77 (0 rows)
    - In total, 17,654 rows go to quarantine and 16,808,386 rows go to silver, which adds up to the 16,826,040 rows downloaded.
- The star schema has one fact table and four dimension tables:
    - `fact_trips`: one row per trip, with the measures (`trip_seconds`, `trip_miles`, `fare`, `tips`, `tolls`, `extras`, `trip_total`) and the keys to the dimensions
    - `dim_date`: one row per day (`date`, `year`, `month`, `day`, `day_name`, `is_weekend`), joined on `trip_date`
    - `dim_community_area`: the 77 official areas plus `0 = Unknown` for trips outside Chicago, joined on `pickup_community_area_id` and `dropoff_community_area_id`
    - `dim_payment_type`: the 8 payment types with `tips_recorded`, joined on `payment_type_id`
    - `dim_company`: the 52 companies with a clean display name, joined on `company_id`
- I used local Spark and the LocalExecutor for Airflow, as this is a test and everything runs on one laptop.
