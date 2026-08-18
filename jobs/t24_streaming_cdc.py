# 2026-06-22: support :ss=SS_KEY override for tables where NAME != SS RECID (e.g. ACCOUNT.HIS -> ACCOUNT)
# 2026-06-26: rebuild spark-t24 image để deploy fix :ss= (image v3 cũ hơn fix) + stream HOLD.CONTROL (luồng sftp_hold)
"""CDC-path entrypoint: Kafka (Debezium) -> in-stream parse -> Bronze.

One long-running SparkApplication consumes all CDC topics; each topic maps
to one table spec. foreachBatch turns every micro-batch into a static
DataFrame, so the EXACT same parse() as the pull path runs here.

Per micro-batch, per table:
    events -> latest_per_recid (LSN)  -> split deletes / upserts
           -> parse(upserts)          -> MERGE Bronze + replace children
           -> apply deletes           -> errors -> DLQ
    + upsert max(__lsn) per table (+ __global__) -> etl_stream_progress
All sink steps are idempotent => checkpoint replay after restart is safe.

COB gate support (arch/BNCTL_DWH_Daily_Pipeline_Design.md §3,§5):
    --keep-lsn-tables BATCH   : giữ cột __lsn trên bronze (cần cho L_cob).
    etl_stream_progress(table_name, max_lsn): nhánh A của gate (max_lsn>=L_cob)
      + stream_lsn (__global__). Nhánh B (lag) tính NGOÀI luồng — Spark cluster
      mode không expose lastProgress/listener cho Python (xem scenario_branch_b.py).
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, functions as F

from t24_parser.core import parse
from t24_parser.metadata import build_spec
from t24_parser.sinks import latest_per_recid, split_deletes, write_bronze
from t24_parser.sources import read_cdc_stream, unwrap_debezium


def _ensure_progress_table(spark: SparkSession, table: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            table_name string,
            max_lsn    string,
            updated_at timestamp
        ) USING iceberg
    """)


def _upsert_progress(spark: SparkSession, table: str, rows: list) -> None:
    """rows=[(table_name, max_lsn)]; MERGE giữ max_lsn LỚN NHẤT (so chuỗi)."""
    if not rows:
        return
    src = spark.createDataFrame(rows, "table_name string, max_lsn string") \
               .withColumn("updated_at", F.current_timestamp())
    src.createOrReplaceTempView("__src_progress")
    spark.sql(f"""
        MERGE INTO {table} t USING __src_progress s
        ON t.table_name = s.table_name
        WHEN MATCHED AND s.max_lsn > t.max_lsn THEN UPDATE SET
            t.max_lsn = s.max_lsn, t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT (table_name, max_lsn, updated_at)
            VALUES (s.table_name, s.max_lsn, s.updated_at)
    """)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--ss", required=True, help="SS json (s3a://... readable by spark)")
    ap.add_argument("--checkpoint", required=True)
    ap.add_argument("--trigger", default="30 seconds")
    ap.add_argument("--starting-offsets", default="earliest")
    ap.add_argument("--dlq", default=None)
    ap.add_argument("--progress-table", default="hive.bronze.etl_stream_progress")
    ap.add_argument("--keep-lsn-tables", default="",
                    help="comma list of SS table names to carry __lsn into Bronze")
    ap.add_argument("--tables", nargs="+", default=[],
                    help="TABLE=topic:bronze_table mappings (hoac dung --tables-file)")
    ap.add_argument("--tables-file", default=None,
                    help="file moi dong 1 mapping TABLE=topic:bronze_table; '#'=comment, "
                         "dong trong bo qua (interface cho dev: ConfigMap t24-cdc-tables)")
    args = ap.parse_args()

    table_specs = list(args.tables)
    if args.tables_file:
        with open(args.tables_file) as fh:
            table_specs += [ln.split("#", 1)[0].strip() for ln in fh]
    table_specs = [s for s in table_specs if s]
    if not table_specs:
        raise SystemExit("can --tables hoac --tables-file (it nhat 1 bang)")

    spark = SparkSession.builder.appName("t24-cob-streaming").getOrCreate()
    _ensure_progress_table(spark, args.progress_table)

    ss_rows = spark.read.option("multiLine", "true").json(args.ss).collect()
    ss = {r["RECID"]: r["XMLRECORD"] for r in ss_rows}
    keep_lsn = {t.strip() for t in args.keep_lsn_tables.split(",") if t.strip()}

    routes = {}  # topic -> (table, spec, bronze, keep_lsn)
    for m in table_specs:
        table, rest = m.split("=", 1)
        # Optional :ss=SS_KEY override — dùng khi logical NAME ≠ SS RECID
        # e.g. ACCOUNT.HIS=topic:bronze:ss=ACCOUNT
        if ":ss=" in rest:
            topic_bronze, ss_override = rest.rsplit(":ss=", 1)
            topic, bronze = topic_bronze.split(":", 1)
            ss_key = ss_override.strip()
        else:
            topic, bronze = rest.split(":", 1)
            ss_key = table
        routes[topic] = (table, build_spec(ss_key, ss), bronze, table in keep_lsn)

    stream = read_cdc_stream(spark, args.bootstrap, list(routes),
                             starting_offsets=args.starting_offsets)

    def sink(batch_df: DataFrame, epoch: int) -> None:
        batch_df.persist()
        try:
            progress = []
            global_max = None
            for topic, (table, spec, bronze, klsn) in routes.items():
                part = batch_df.filter(F.col("topic") == topic)
                if part.isEmpty():
                    continue
                events = latest_per_recid(unwrap_debezium(part))
                mx = events.agg(F.max("__lsn").alias("m")).collect()[0]["m"]
                if mx is not None:
                    progress.append((bronze, mx))
                    if global_max is None or mx > global_max:
                        global_max = mx
                upserts, deletes = split_deletes(events)
                frames = parse(upserts, spec)
                if klsn:
                    frames.main = frames.main.join(
                        events.select("RECID", F.col("__lsn")), "RECID", "left")
                write_bronze(frames, bronze, deletes=deletes, dlq_path=args.dlq)

            if global_max is not None:
                progress.append(("__global__", global_max))
            _upsert_progress(spark, args.progress_table, progress)
        finally:
            batch_df.unpersist()

    (stream.writeStream
        .foreachBatch(sink)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
        .awaitTermination())


if __name__ == "__main__":
    main()
