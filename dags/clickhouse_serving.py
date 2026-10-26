import logging
from datetime import timedelta
from pathlib import Path

import clickhouse_connect
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Asset, BaseHook, dag, get_current_context, task

GOLD_ASSET = Asset("s3://gold/")
SCHEMA_SQL = Path(__file__).parent / "sql" / "clickhouse_schema.sql"

INSERT_FACT = """
INSERT INTO fact_trips
SELECT
    trip_id,
    taxi_id,
    trip_date,
    trip_start_timestamp,
    trip_end_timestamp,
    pickup_community_area_id,
    dropoff_community_area_id,
    payment_type_id,
    company_id,
    trip_seconds,
    trip_miles,
    fare,
    tips,
    tolls,
    extras,
    trip_total
FROM s3(%(url)s, %(key)s, %(secret)s, 'Parquet')
"""

log = logging.getLogger(__name__)


def clickhouse():
    conn = BaseHook.get_connection("clickhouse")
    return clickhouse_connect.get_client(
        host=conn.host, port=conn.port, username=conn.login, password=conn.password, database=conn.schema
    )


def gold_files(path: str) -> dict:
    minio = BaseHook.get_connection("minio")
    return {"url": f"{minio.extra_dejson['endpoint_url']}/gold/{path}", "key": minio.login, "secret": minio.password}


@task(retries=2, retry_delay=timedelta(minutes=1))
def create_schema() -> None:
    client = clickhouse()
    for statement in SCHEMA_SQL.read_text().split(";"):
        if statement.strip():
            client.command(statement)


@task(retries=2, retry_delay=timedelta(minutes=1))
def gold_months() -> list[str]:
    context = get_current_context()
    if context["dag_run"].run_type == "manual":
        keys = S3Hook(aws_conn_id="minio").list_keys(bucket_name="gold", prefix="fact_trips/trip_month=")
        months = {key.split("trip_month=")[1][:7] for key in keys}
    else:
        months = {
            month
            for events in context["triggering_asset_events"].values()
            for event in events
            for month in (event.extra or {}).get("months", [])
        }
    log.info("clickhouse will reload %d months", len(months))
    return sorted(months)


@task(retries=2, retry_delay=timedelta(minutes=1))
def load_dimensions() -> dict:
    client = clickhouse()
    counts = {}
    for table in ("dim_community_area", "dim_date", "dim_payment_type", "dim_company"):
        client.command(f"TRUNCATE TABLE {table}")
        client.command(
            f"INSERT INTO {table} SELECT * FROM s3(%(url)s, %(key)s, %(secret)s, 'Parquet')",
            parameters=gold_files(f"{table}/*.parquet"),
        )
        counts[table] = client.command(f"SELECT count() FROM {table}")
    log.info("clickhouse dimensions loaded %s", counts)
    return counts


@task(retries=2, retry_delay=timedelta(minutes=1))
def load_fact_months(months: list[str]) -> dict:
    client = clickhouse()
    report = {}
    for month in months:
        partition = month.replace("-", "")
        files = gold_files(f"fact_trips/trip_month={month}/*.parquet")

        client.command(f"ALTER TABLE fact_trips DROP PARTITION {partition}")
        client.command(INSERT_FACT, parameters=files)

        counts = {
            "gold": client.command("SELECT count() FROM s3(%(url)s, %(key)s, %(secret)s, 'Parquet')", parameters=files),
            "clickhouse": client.command(f"SELECT count() FROM fact_trips WHERE toYYYYMM(trip_date) = {partition}"),
        }
        if counts["gold"] != counts["clickhouse"]:
            raise ValueError(f"reconciliation failed for {month}: {counts}")
        report[month] = counts
        log.info("clickhouse month loaded %s %s", month, counts)
    return report


@dag(dag_id="clickhouse_serving", schedule=[GOLD_ASSET], max_active_runs=1, tags=["serving"])
def clickhouse_serving():
    create_schema() >> [load_dimensions(), load_fact_months(gold_months())]


clickhouse_serving()
