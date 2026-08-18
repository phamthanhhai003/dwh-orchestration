"""Output adapters: ParsedFrames -> Bronze (Iceberg).

The ONLY place where CDC and Pull differ semantically:
  - pull batch  : snapshot semantics - MERGE upsert by recid
  - cdc stream  : the JOB dedupes events to latest-per-recid by __lsn and
                  splits deletes (after=null, no XMLRECORD -> nothing to
                  parse) from upserts BEFORE calling parse(); this module
                  then applies upserts (MERGE) and deletes (DELETE) — both
                  idempotent, safe for foreachBatch replays after restart.

Iceberg specifics (catalog, table properties, partitioning) intentionally
live here and nowhere else.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from .core import ParsedFrames


def latest_per_recid(df: DataFrame, order_col: str = "__lsn") -> DataFrame:
    """CDC micro-batches may carry several events per RECID - keep the last."""
    w = Window.partitionBy("RECID").orderBy(F.col(order_col).desc_nulls_last())
    return (df.withColumn("__rn", F.row_number().over(w))
              .filter(F.col("__rn") == 1).drop("__rn"))


def split_deletes(events: DataFrame) -> tuple:
    """(upserts, delete_recids) from deduped Debezium events.

    Deletes carry no XMLRECORD (after=null) so they must never reach
    parse() - they are applied directly as Bronze DELETEs.
    """
    deletes = (events.filter(F.col("__op") == "d")
                     .select(F.col("RECID").alias("recid")).distinct())
    upserts = events.filter((F.col("__op").isNull()) | (F.col("__op") != "d")) \
                    .select("RECID", "XMLRECORD")
    return upserts, deletes


def _ensure_table(df: DataFrame, target: str) -> None:
    if not df.sparkSession.catalog.tableExists(target):
        df.limit(0).writeTo(target).using("iceberg").create()


def _merge_upsert(main: DataFrame, target: str) -> None:
    """Idempotent MERGE of the parsed main frame into Bronze on recid."""
    spark = main.sparkSession
    _ensure_table(main, target)
    main.createOrReplaceTempView("__src_main")
    cols = main.columns
    sets = ", ".join(f"t.{c} = s.{c}" for c in cols if c != "recid")
    spark.sql(f"""
        MERGE INTO {target} t
        USING __src_main s
        ON t.recid = s.recid
        WHEN MATCHED THEN UPDATE SET {sets}
        WHEN NOT MATCHED THEN INSERT ({", ".join(cols)})
        VALUES ({", ".join("s." + c for c in cols)})
    """)


def _replace_children(child: DataFrame, target: str) -> None:
    """Child long tables: replace all rows belonging to the touched recids."""
    spark = child.sparkSession
    _ensure_table(child, target)
    child.createOrReplaceTempView("__src_child")
    spark.sql(f"""
        DELETE FROM {target}
        WHERE recid IN (SELECT DISTINCT recid FROM __src_child)
    """)
    child.writeTo(target).append()


def _ensure_table_partitioned(df: DataFrame, target: str, part_col: str) -> None:
    if not df.sparkSession.catalog.tableExists(target):
        (df.limit(0).writeTo(target).using("iceberg")
           .partitionedBy(F.col(part_col)).create())


def _delete_recids(recids: DataFrame, target: str) -> None:
    spark = recids.sparkSession
    if not spark.catalog.tableExists(target):
        return
    recids.createOrReplaceTempView("__del_recids")
    spark.sql(f"DELETE FROM {target} "
              f"WHERE recid IN (SELECT recid FROM __del_recids)")


def write_bronze(frames: ParsedFrames, table_prefix: str,
                 deletes: Optional[DataFrame] = None,
                 dlq_path: Optional[str] = None) -> None:
    """Write ParsedFrames (+ optional CDC deletes) to Bronze.

    table_prefix e.g. 'hive.bronze.t24_account' produces:
        <prefix>        main - 1 row per recid; multi-value fields are
                        dense typed ARRAY columns right in this table
        <prefix>__sv    rare sub-value (sv>1) spillover, long format
    Every step is idempotent => safe under foreachBatch replay.
    """
    spark = frames.main.sparkSession
    main = frames.main
    sv = frames.sv_long
    # only add business_date if target already has it (created by pull path)
    if spark.catalog.tableExists(table_prefix):
        tgt_cols = [f.name for f in spark.table(table_prefix).schema]
        if "business_date" in tgt_cols and "business_date" not in main.columns:
            main = main.withColumn("business_date", F.lit(None).cast("date"))
    sv_target = f"{table_prefix}__sv"
    if spark.catalog.tableExists(sv_target):
        sv_cols = [f.name for f in spark.table(sv_target).schema]
        if "business_date" in sv_cols and "business_date" not in sv.columns:
            sv = sv.withColumn("business_date", F.lit(None).cast("date"))
    _merge_upsert(main, table_prefix)
    _replace_children(sv, sv_target)

    if deletes is not None:
        for t in (table_prefix, f"{table_prefix}__sv"):
            _delete_recids(deletes, t)

    if dlq_path is not None and frames.errors.limit(1).count() > 0:
        frames.errors.write.mode("append").parquet(dlq_path)


def write_bronze_pull(frames: ParsedFrames, table_prefix: str,
                      business_date: str,
                      dlq_path: Optional[str] = None) -> int:
    """PULL-path write: stamp business_date=D + ghi PARTITION theo business_date.

    Khác write_bronze (CDC = MERGE current-state): pull ghi bất biến theo từng D,
    dbt đọc `WHERE business_date=D` (xem PIPELINE_ORCHESTRATION_DESIGN §3-4).
    Dùng overwritePartitions() → chỉ ghi đè đúng partition D → idempotent khi rerun.
    Trả số row main đã ghi (cho reconcile/seal).
    """
    bd = F.to_date(F.lit(business_date))
    main = frames.main.withColumn("business_date", bd)
    _ensure_table_partitioned(main, table_prefix, "business_date")
    main.writeTo(table_prefix).overwritePartitions()

    sv_target = f"{table_prefix}__sv"
    sv = frames.sv_long.withColumn("business_date", bd)
    _ensure_table_partitioned(sv, sv_target, "business_date")
    sv.writeTo(sv_target).overwritePartitions()

    if dlq_path is not None and frames.errors.limit(1).count() > 0:
        frames.errors.write.mode("append").parquet(dlq_path)

    return main.count()
