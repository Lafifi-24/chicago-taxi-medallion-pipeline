import logging
import os
from datetime import timedelta

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Asset, dag, get_current_context, task
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SILVER_ASSET = Asset("s3://silver/trips/")
GOLD_ASSET = Asset("s3://gold/")

COMMUNITY_AREAS_URL = "https://data.cityofchicago.org/resource/igwz-8jzy.csv"
COMMUNITY_AREAS_KEY = "reference/community_areas.csv"
UNKNOWN_AREA = 0
TIPS_RECORDED = ["Credit Card", "Mobile"]
LICENSE_PREFIX = r"^\d+ - (\d+ )?"

log = logging.getLogger(__name__)


def dim_community_area(spark: SparkSession, path: str) -> DataFrame:
    areas = spark.read.option("header", True).csv(path).select(
        F.col("area_numbe").cast("int").alias("community_area_id"),
        F.initcap("community").alias("community_area_name"),
    )
    unknown = spark.createDataFrame([(UNKNOWN_AREA, "Unknown")], areas.schema)
    return areas.unionByName(unknown)


def dim_date(silver: DataFrame) -> DataFrame:
    bounds = silver.agg(F.min("trip_date").alias("first_day"), F.max("trip_date").alias("last_day"))
    days = bounds.select(F.explode(F.sequence("first_day", "last_day")).alias("date"))
    return days.select(
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def distinct_names(silver: DataFrame, column: str) -> list[str]:
    return [row[0] for row in silver.select(F.coalesce(column, F.lit("Unknown"))).distinct().collect()]


def assign_ids(spark: SparkSession, path: str, id_column: str, name_column: str, names: list[str]) -> dict[str, int]:
    try:
        ids = {row[name_column]: row[id_column] for row in spark.read.parquet(path).collect()}
    except AnalysisException:
        ids = {}
    next_id = max(ids.values(), default=0) + 1
    for name in sorted(set(names) - set(ids)):
        ids[name] = next_id
        next_id += 1
    return ids


def id_lookup(ids: dict[str, int], column: str):
    mapping = F.create_map(*[F.lit(value) for pair in ids.items() for value in pair])
    return mapping[F.coalesce(column, F.lit("Unknown"))]


def dim_payment_type(spark: SparkSession, ids: dict[str, int]) -> DataFrame:
    rows = [(number, name, name in TIPS_RECORDED) for name, number in sorted(ids.items(), key=lambda item: item[1])]
    return spark.createDataFrame(rows, "payment_type_id int, payment_type string, tips_recorded boolean")


def dim_company(spark: SparkSession, ids: dict[str, int]) -> DataFrame:
    rows = [(number, name) for name, number in sorted(ids.items(), key=lambda item: item[1])]
    companies = spark.createDataFrame(rows, "company_id int, company string")
    return companies.withColumn("company_name", F.regexp_replace("company", LICENSE_PREFIX, ""))


def fact_trips(silver_month: DataFrame, payment_ids: dict[str, int], company_ids: dict[str, int]) -> DataFrame:
    return silver_month.select(
        "trip_id",
        "taxi_id",
        "trip_date",
        "trip_start_timestamp",
        "trip_end_timestamp",
        F.coalesce("pickup_community_area", F.lit(UNKNOWN_AREA)).alias("pickup_community_area_id"),
        F.coalesce("dropoff_community_area", F.lit(UNKNOWN_AREA)).alias("dropoff_community_area_id"),
        id_lookup(payment_ids, "payment_type").alias("payment_type_id"),
        id_lookup(company_ids, "company").alias("company_id"),
        "trip_seconds",
        "trip_miles",
        "fare",
        "tips",
        "tolls",
        "extras",
        "trip_total",
    )


@task(retries=2, retry_delay=timedelta(minutes=1))
def community_areas() -> str:
    s3 = S3Hook(aws_conn_id="minio")
    hard = os.environ.get("BRONZE_WRITE_MODE", "soft") == "hard"
    if hard or not s3.check_for_key(COMMUNITY_AREAS_KEY, bucket_name="bronze"):
        response = requests.get(COMMUNITY_AREAS_URL, params={"$select": "area_numbe,community", "$limit": 100}, timeout=120)
        response.raise_for_status()
        s3.load_bytes(response.content, key=COMMUNITY_AREAS_KEY, bucket_name="bronze", replace=True)
    return f"s3a://bronze/{COMMUNITY_AREAS_KEY}"


@task(retries=2, retry_delay=timedelta(minutes=1))
def silver_months() -> list[str]:
    context = get_current_context()
    if context["dag_run"].run_type == "manual":
        keys = S3Hook(aws_conn_id="minio").list_keys(bucket_name="silver", prefix="trips/trip_date=")
        days = {key.split("trip_date=")[1][:10] for key in keys}
    else:
        days = {
            day
            for events in context["triggering_asset_events"].values()
            for event in events
            for day in (event.extra or {}).get("days", [])
        }
    months = sorted({day[:7] for day in days})
    log.info("gold will rebuild %d months", len(months))
    return months


@task(retries=2, retry_delay=timedelta(minutes=1), outlets=[GOLD_ASSET])
def build_gold(months: list[str], areas_path: str) -> dict:
    if not months:
        return {}

    spark = SparkSession.builder.appName("gold_transformation").getOrCreate()
    report = {}
    try:
        silver = spark.read.parquet("s3a://silver/trips")
        dim_community_area(spark, areas_path).coalesce(1).write.mode("overwrite").parquet("s3a://gold/dim_community_area")
        dim_date(silver).coalesce(1).write.mode("overwrite").parquet("s3a://gold/dim_date")
        payment_ids = assign_ids(spark, "s3a://gold/dim_payment_type", "payment_type_id", "payment_type", distinct_names(silver, "payment_type"))
        company_ids = assign_ids(spark, "s3a://gold/dim_company", "company_id", "company", distinct_names(silver, "company"))
        dim_payment_type(spark, payment_ids).coalesce(1).write.mode("overwrite").parquet("s3a://gold/dim_payment_type")
        dim_company(spark, company_ids).coalesce(1).write.mode("overwrite").parquet("s3a://gold/dim_company")

        for month in months:
            silver_month = silver.filter(F.date_format("trip_date", "yyyy-MM") == month)
            path = f"s3a://gold/fact_trips/trip_month={month}"
            fact_trips(silver_month, payment_ids, company_ids).coalesce(1).write.mode("overwrite").parquet(path)

            counts = {"silver": silver_month.count(), "fact": spark.read.parquet(path).count()}
            if counts["silver"] != counts["fact"]:
                raise ValueError(f"reconciliation failed for {month}: {counts}")
            report[month] = counts
            log.info("gold month done %s %s", month, counts)
    finally:
        spark.stop()

    get_current_context()["outlet_events"][GOLD_ASSET].extra = {"months": months}
    return report


@dag(dag_id="gold_transformation", schedule=[SILVER_ASSET], max_active_runs=1, tags=["gold"])
def gold_transformation():
    build_gold(silver_months(), community_areas())


gold_transformation()
