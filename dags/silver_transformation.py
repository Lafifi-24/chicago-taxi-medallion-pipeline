import logging
from datetime import timedelta
from itertools import groupby

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Asset, dag, get_current_context, task
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

BRONZE_ASSET = Asset("s3://bronze/")
SILVER_ASSET = Asset("s3://silver/trips/")

SILVER_BUCKET = "silver"
OUTPUTS = ("trips", "quarantine")

BRONZE_SCHEMA = StructType(
    [
        StructField("trip_id", StringType()),
        StructField("taxi_id", StringType()),
        StructField("trip_start_timestamp", TimestampNTZType()),
        StructField("trip_end_timestamp", TimestampNTZType()),
        StructField("trip_seconds", IntegerType()),
        StructField("trip_miles", DoubleType()),
        StructField("pickup_census_tract", StringType()),
        StructField("dropoff_census_tract", StringType()),
        StructField("pickup_community_area", IntegerType()),
        StructField("dropoff_community_area", IntegerType()),
        StructField("fare", DoubleType()),
        StructField("tips", DoubleType()),
        StructField("tolls", DoubleType()),
        StructField("extras", DoubleType()),
        StructField("trip_total", DoubleType()),
        StructField("payment_type", StringType()),
        StructField("company", StringType()),
        StructField("pickup_centroid_latitude", DoubleType()),
        StructField("pickup_centroid_longitude", DoubleType()),
        StructField("pickup_centroid_location", StringType()),
        StructField("dropoff_centroid_latitude", DoubleType()),
        StructField("dropoff_centroid_longitude", DoubleType()),
        StructField("dropoff_centroid_location", StringType()),
        StructField("_corrupt_record", StringType()),
    ]
)

AMOUNT_COLUMNS = ["trip_seconds", "trip_miles", "fare", "tips", "tolls", "extras", "trip_total"]
COMMUNITY_AREA_COLUMNS = ["pickup_community_area", "dropoff_community_area"]

log = logging.getLogger(__name__)


def read_bronze(spark: SparkSession, days: list[str]) -> DataFrame:
    paths = [f"s3a://bronze/{day[:4]}/{day[5:7]}/{day[8:]}.csv" for day in days]
    return (
        spark.read.schema(BRONZE_SCHEMA)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(paths)
        .withColumn(
            "trip_date",
            F.to_date(F.regexp_extract("_metadata.file_path", r"(\d{4}/\d{2}/\d{2})\.csv$", 1), "yyyy/MM/dd"),
        )
    )


def quarantine_reason():
    return (
        F.when(F.col("_corrupt_record").isNotNull(), "unparseable_row")
        .when(F.col("trip_id").isNull(), "missing_trip_id")
        .when(F.col("trip_start_timestamp").isNull() | F.col("trip_end_timestamp").isNull(), "missing_timestamp")
        .when(F.col("trip_end_timestamp") < F.col("trip_start_timestamp"), "end_before_start")
        .when(F.col("trip_total").isNull(), "missing_trip_total")
        .when(F.least(*AMOUNT_COLUMNS) < 0, "negative_value")
        .when(F.greatest(*COMMUNITY_AREA_COLUMNS) > 77, "invalid_community_area")
        .when(F.least(*COMMUNITY_AREA_COLUMNS) < 1, "invalid_community_area")
    )


def count_by_day(df: DataFrame) -> dict[str, int]:
    return {str(row["trip_date"]): row["count"] for row in df.groupBy("trip_date").count().collect()}


def reconcile(batch: dict, trips: DataFrame, valid: DataFrame, clean: DataFrame, quarantine: DataFrame) -> dict:
    read, kept, deduped, rejected = (count_by_day(df) for df in (trips, valid, clean, quarantine))
    report = {}
    for day, bronze_rows in batch.items():
        counts = {
            "bronze": bronze_rows,
            "read": read.get(day, 0),
            "silver": deduped.get(day, 0),
            "quarantine": rejected.get(day, 0),
            "duplicates": kept.get(day, 0) - deduped.get(day, 0),
        }
        lost_between_layers = bronze_rows is not None and counts["read"] != bronze_rows
        unaccounted = counts["read"] != counts["silver"] + counts["quarantine"] + counts["duplicates"]
        if lost_between_layers or unaccounted:
            raise ValueError(f"reconciliation failed for {day}: {counts}")
        report[day] = counts
    return report


def clear_days(s3: S3Hook, days: list[str]) -> None:
    for output in OUTPUTS:
        for day in days:
            keys = s3.list_keys(bucket_name=SILVER_BUCKET, prefix=f"{output}/trip_date={day}/")
            if keys:
                s3.delete_objects(bucket=SILVER_BUCKET, keys=keys)


def process_batch(spark: SparkSession, s3: S3Hook, batch: dict) -> dict:
    trips = read_bronze(spark, list(batch)).withColumn("quarantine_reason", quarantine_reason()).cache()
    valid = trips.filter(F.col("quarantine_reason").isNull()).drop("_corrupt_record", "quarantine_reason")
    clean = valid.dropDuplicates(["trip_id"]).cache()
    quarantine = trips.filter(F.col("quarantine_reason").isNotNull())

    report = reconcile(batch, trips, valid, clean, quarantine)
    clear_days(s3, list(batch))
    clean.repartition("trip_date").write.mode("append").partitionBy("trip_date").parquet(f"s3a://{SILVER_BUCKET}/trips")
    quarantine.repartition("trip_date").write.mode("append").partitionBy("trip_date").parquet(
        f"s3a://{SILVER_BUCKET}/quarantine"
    )

    trips.unpersist()
    clean.unpersist()
    return report


@task(retries=2, retry_delay=timedelta(minutes=1))
def bronze_days() -> dict:
    context = get_current_context()
    if context["dag_run"].run_type == "manual":
        keys = S3Hook(aws_conn_id="minio").list_keys(bucket_name="bronze")
        days = {f"{key[:4]}-{key[5:7]}-{key[8:10]}": None for key in keys if key.endswith(".csv")}
    else:
        days = {}
        for events in context["triggering_asset_events"].values():
            for event in events:
                extra = event.extra or {}
                if extra.get("status") in ("written", "replaced"):
                    days[extra["day"]] = extra["rows"]
    log.info("silver will process %d bronze days", len(days))
    return dict(sorted(days.items()))


@task(retries=2, retry_delay=timedelta(minutes=1), outlets=[SILVER_ASSET])
def transform(days: dict) -> dict:
    if not days:
        return {}

    s3 = S3Hook(aws_conn_id="minio")
    spark = SparkSession.builder.appName("silver_transformation").getOrCreate()
    report = {}
    try:
        for _, month in groupby(days, key=lambda day: day[:7]):
            batch = {day: days[day] for day in month}
            report.update(process_batch(spark, s3, batch))
            log.info("silver batch done %s", {day: report[day] for day in batch})
    finally:
        spark.stop()

    get_current_context()["outlet_events"][SILVER_ASSET].extra = {"days": list(days)}
    return report


@dag(dag_id="silver_transformation", schedule=[BRONZE_ASSET], max_active_runs=1, tags=["silver"])
def silver_transformation():
    transform(bronze_days())


silver_transformation()
