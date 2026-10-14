"""Glue job: pull Salesforce Marketing Cloud Engagement entities into Amazon S3 Tables.

Reads each entity through the native Glue `salesforcemarketingcloud` connector and
MERGEs it into an Iceberg table in an S3 table bucket (via the S3 Tables Iceberg
REST endpoint). Event entities load an overlapping incremental window; dimension
entities are read in full and upserted. Reruns are idempotent.

Pure helpers live at module level so they can be unit tested without Glue/Spark.
"""

import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

CATALOG = "s3tables"


@dataclass(frozen=True)
class Entity:
    name: str  # connector ENTITY_NAME
    table: str  # target Iceberg table
    keys: tuple  # natural key columns (snake_case, after normalization)
    event_date: str | None = None  # source date field for incremental events


EVENT_KEYS = ("send_id", "subscriber_key", "event_date", "event_type")

ENTITIES = {
    e.table: e
    for e in (
        Entity("SentEvent", "sent_event", EVENT_KEYS, "EventDate"),
        Entity("OpenEvent", "open_event", EVENT_KEYS, "EventDate"),
        Entity("ClickEvent", "click_event", EVENT_KEYS + ("url",), "EventDate"),
        Entity("BounceEvent", "bounce_event", EVENT_KEYS, "EventDate"),
        Entity("UnsubEvent", "unsub_event", EVENT_KEYS, "EventDate"),
        Entity("NotSentEvent", "not_sent_event", EVENT_KEYS, "EventDate"),
        Entity("Send", "send", ("id",)),
        Entity("Subscriber", "subscriber", ("id",)),
        Entity("List", "list", ("id",)),
        Entity("Email", "email", ("id",)),
    )
}


def to_snake(name: str) -> str:
    """SubscriberKey -> subscriber_key, SendID -> send_id, URL -> url."""
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.strip("_").lower()


def window(now: datetime, lookback_days: int) -> tuple[str, str]:
    """[lower, upper) bounds in the connector's timestamp format.

    Upper is the next UTC midnight so today's events are included; the lookback
    overlaps previous runs to pick up late-arriving events (MERGE dedupes them).
    """
    today = now.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    upper = today + timedelta(days=1)
    lower = today - timedelta(days=lookback_days)
    fmt = "%Y-%m-%dT%H:%M:%S.000Z"
    return lower.strftime(fmt), upper.strftime(fmt)


def merge_sql(
    table: str, source_view: str, keys: tuple, columns: list[str], update: bool, target_since: str | None = None
) -> str:
    """target_since bounds the target scan to the incremental window's partitions (events only)."""
    target = f"{CATALOG}.sfmc.{table}"
    on = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
    if target_since:
        on += f" AND t.event_date >= TIMESTAMP '{target_since}'"
    cols = ", ".join(columns)
    vals = ", ".join(f"s.{c}" for c in columns)
    sql = f"MERGE INTO {target} t USING {source_view} s ON {on}"
    if update:
        sets = ", ".join(f"t.{c} = s.{c}" for c in columns if c not in keys)
        sql += f" WHEN MATCHED THEN UPDATE SET {sets}"
    return sql + f" WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"


def parse_list(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def materialize(df):
    """Cache before counting so the connector is read once, not again by the write."""
    df = df.persist()
    return df, df.count()


# --------------------------------------------------------------------------- Spark


def normalize(df):
    """snake_case columns, structs/arrays to JSON strings, add ingestion metadata."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, MapType, StructType

    cols = []
    for field in df.schema.fields:
        col = F.col(f"`{field.name}`")
        if isinstance(field.dataType, (StructType, ArrayType, MapType)):
            col = F.to_json(col)
        cols.append(col.alias(to_snake(field.name)))
    return df.select(*cols).withColumn("_ingested_at", F.current_timestamp())


def add_missing_columns(spark, table: str, df) -> None:
    """Evolve the Iceberg schema when SFMC exposes new fields."""
    target = f"{CATALOG}.sfmc.{table}"
    existing = {f.name for f in spark.table(target).schema.fields}
    for field in df.schema.fields:
        if field.name not in existing:
            spark.sql(f"ALTER TABLE {target} ADD COLUMN {field.name} {field.dataType.simpleString()}")


def load_entity(glue_context, spark, entity: Entity, args: dict, bounds: tuple) -> int:
    from pyspark.sql import functions as F

    options = {
        "connectionName": args["CONNECTION_NAME"],
        "ENTITY_NAME": entity.name,
        "API_VERSION": "v1",
        "INSTANCE_URL": args["SOAP_INSTANCE_URL"],
    }
    if entity.event_date:
        options.update(
            PARTITION_FIELD=entity.event_date,
            LOWER_BOUND=bounds[0],
            UPPER_BOUND=bounds[1],
            NUM_PARTITIONS=args["NUM_PARTITIONS"],
        )

    df = glue_context.create_dynamic_frame.from_options(
        connection_type="salesforcemarketingcloud", connection_options=options
    ).toDF()
    df = normalize(df)
    if entity.event_date:
        df = df.withColumn("event_date", F.to_timestamp("event_date")).filter(
            (F.col("event_date") >= F.to_timestamp(F.lit(bounds[0])))
            & (F.col("event_date") < F.to_timestamp(F.lit(bounds[1])))
        )
    df = df.dropDuplicates(list(entity.keys))

    df, rows = materialize(df)
    try:
        if rows == 0:
            return 0

        target = f"{CATALOG}.sfmc.{entity.table}"
        if not spark.catalog.tableExists(target):
            writer = df.writeTo(target).using("iceberg")
            if entity.event_date:
                writer = writer.partitionedBy(F.days("event_date"))
            writer.create()
            return rows

        add_missing_columns(spark, entity.table, df)
        view = f"src_{entity.table}"
        df.createOrReplaceTempView(view)
        since = bounds[0] if entity.event_date else None
        spark.sql(
            merge_sql(entity.table, view, entity.keys, df.columns, update=since is None, target_since=since)
        )
        return rows
    finally:
        df.unpersist()


def main() -> None:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "CONNECTION_NAME", "SOAP_INSTANCE_URL", "ENTITIES", "LOOKBACK_DAYS", "NUM_PARTITIONS"],
    )
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.sfmc")
    bounds = window(datetime.now(timezone.utc), int(args["LOOKBACK_DAYS"]))
    print(f"Incremental window for events: [{bounds[0]}, {bounds[1]})")

    failures = []
    for table in parse_list(args["ENTITIES"]):
        try:
            rows = load_entity(glue_context, spark, ENTITIES[table], args, bounds)
            print(f"{table}: merged {rows} rows")
        except Exception as exc:  # keep loading other entities, fail the run at the end
            print(f"{table}: FAILED {exc!r}")
            failures.append(table)

    job.commit()
    if failures:
        raise RuntimeError(f"Entities failed: {failures}")


if __name__ == "__main__":
    main()
