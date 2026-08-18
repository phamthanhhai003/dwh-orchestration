"""Input adapters. The ONLY place that knows where bytes come from.

Both adapters end at the same contract the core expects:
    columns RECID, XMLRECORD (+ CDC passthrough __op, __lsn, __ts_ms)
"""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F

CDC_PASSTHROUGH = ("__op", "__lsn", "__ts_ms")


# --------------------------------------------------------------------------
# CDC path: Kafka + Debezium SQL Server envelope
# --------------------------------------------------------------------------

def read_cdc_stream(spark: SparkSession, bootstrap: str, topics: Sequence[str],
                    starting_offsets: str = "earliest") -> DataFrame:
    """Streaming read of Debezium topics. Returns raw Kafka frame."""
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", ",".join(topics))
            .option("startingOffsets", starting_offsets)
            .load())


def unwrap_debezium(kafka_df: DataFrame) -> DataFrame:
    """Debezium JSON envelope -> (RECID, XMLRECORD, __op, __lsn, __ts_ms).

    Handles both JsonConverter shapes (with/without schemas.enable wrapper):
    fields live at $.payload.* or at $.*. For deletes (op='d') `after` is
    null - RECID comes from `before`, XMLRECORD stays null and the sink
    turns it into a Bronze DELETE. Kafka tombstones (value=null) are dropped
    (the preceding op='d' event carries the delete).
    """
    v = F.col("value").cast("string")

    def j(path: str):
        return F.coalesce(F.get_json_object(v, f"$.payload.{path}"),
                          F.get_json_object(v, f"$.{path}"))

    return (kafka_df
            .filter(F.col("value").isNotNull())
            .select(
                F.coalesce(j("after.RECID"), j("before.RECID")).alias("RECID"),
                j("after.XMLRECORD").alias("XMLRECORD"),
                j("op").alias("__op"),
                F.coalesce(j("source.change_lsn"),
                           j("source.commit_lsn")).alias("__lsn"),
                j("ts_ms").cast("long").alias("__ts_ms"),
            )
            .filter(F.col("RECID").isNotNull()))


# --------------------------------------------------------------------------
# Pull path: JDBC (T24 readable secondary) and Raw zone
# --------------------------------------------------------------------------

def read_jdbc(spark: SparkSession, url: str, dbtable: str, user: str,
              password: str, **options) -> DataFrame:
    """Snapshot read of RECID+XMLRECORD from the AlwaysOn readable secondary."""
    reader = (spark.read.format("jdbc")
              .option("url", url)
              .option("dbtable", dbtable)
              .option("user", user)
              .option("password", password))
    for k, val in options.items():
        reader = reader.option(k, val)
    return reader.load().select("RECID", "XMLRECORD")


def read_raw(spark: SparkSession, path: str, fmt: str = "parquet") -> DataFrame:
    """Read an un-parsed Raw zone landing (immutable RECID+XMLRECORD)."""
    return spark.read.format(fmt).load(path).select("RECID", "XMLRECORD")
