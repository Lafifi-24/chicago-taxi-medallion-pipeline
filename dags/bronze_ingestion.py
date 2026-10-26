import csv
import io
import logging
import os
from datetime import date, timedelta
from itertools import groupby

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Asset, TaskGroup, dag, get_current_context, task

START_DATE = date(2021, 1, 1)
END_DATE = date(2023, 12, 31)

SODA_URL = "https://data.cityofchicago.org/resource/wrvz-psew.csv"
PAGE_LIMIT = 50000
BUCKET = "bronze"
WRITE_MODES = ("soft", "hard")
BRONZE_ASSET = Asset("s3://bronze/")

log = logging.getLogger(__name__)


def record(summary: dict) -> dict:
    log.info("bronze load %s", summary)
    get_current_context()["outlet_events"][BRONZE_ASSET].extra = summary
    return summary


@task(retries=3, retry_delay=timedelta(minutes=1), outlets=[BRONZE_ASSET])
def load_day(day: str) -> dict:
    mode = os.environ.get("BRONZE_WRITE_MODE", "soft")
    if mode not in WRITE_MODES:
        raise ValueError(f"BRONZE_WRITE_MODE must be one of {WRITE_MODES}, got {mode!r}")

    current = date.fromisoformat(day)
    key = f"{current:%Y/%m/%d}.csv"
    s3 = S3Hook(aws_conn_id="minio")

    existing = s3.head_object(key=key, bucket_name=BUCKET)
    if mode == "soft" and existing and existing["ContentLength"] > 0:
        return record({"day": day, "status": "skipped", "key": f"{BUCKET}/{key}"})

    next_day = current + timedelta(days=1)
    response = requests.get(
        SODA_URL,
        params={
            "$where": f"trip_start_timestamp >= '{current}T00:00:00' AND trip_start_timestamp < '{next_day}T00:00:00'",
            "$order": ":id",
            "$limit": PAGE_LIMIT,
        },
        timeout=300,
    )
    response.raise_for_status()

    rows = sum(1 for _ in csv.reader(io.StringIO(response.text))) - 1
    if rows >= PAGE_LIMIT:
        raise ValueError(f"{day} returned {rows} rows, the page limit, so the day may be truncated")

    s3.load_bytes(response.content, key=key, bucket_name=BUCKET, replace=True)

    return record(
        {
            "day": day,
            "status": "replaced" if existing else "written",
            "rows": rows,
            "bytes": len(response.content),
            "key": f"{BUCKET}/{key}",
            "query": response.url,
        }
    )


@dag(dag_id="bronze_ingestion", schedule=None, max_active_tasks=4, tags=["bronze"])
def bronze_ingestion():
    days = [START_DATE + timedelta(days=i) for i in range((END_DATE - START_DATE).days + 1)]
    for year, year_days in groupby(days, key=lambda d: d.year):
        with TaskGroup(group_id=str(year)):
            for month, month_days in groupby(year_days, key=lambda d: d.month):
                with TaskGroup(group_id=f"{month:02d}"):
                    for day in month_days:
                        load_day.override(task_id=f"{day.day:02d}")(day.isoformat())


bronze_ingestion()
