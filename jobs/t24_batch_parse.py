"""Pull-path entrypoint: Raw zone (or JDBC) -> parse -> Bronze.

Airflow calls this per table via SparkKubernetesOperator:
    t24_batch_parse.py --table ACCOUNT --source raw \
        --input s3a://raw/t24/FBNK_ACCOUNT/2026-06-11/ \
        --ss s3a://meta/t24/standard_selection.json \
        --target lakehouse.bronze.t24_account
"""

import argparse

from pyspark.sql import SparkSession

from t24_parser.core import parse
from t24_parser.metadata import build_spec
from t24_parser.sinks import write_bronze, write_bronze_pull
from t24_parser.sources import read_raw


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True, help="SS record id, e.g. ACCOUNT")
    ap.add_argument("--input", required=True, help="raw zone path")
    ap.add_argument("--ss", required=True, help="STANDARD.SELECTION json (s3a:// hoặc local)")
    ap.add_argument("--target", required=True, help="Bronze table, e.g. lakehouse.bronze.t24_account")
    ap.add_argument("--business-date", default=None,
                    help="PULL: business_date=D (YYYY-MM-DD) → ghi partition theo D "
                         "(write_bronze_pull). Bỏ trống = MERGE current-state (write_bronze).")
    ap.add_argument("--dlq", default=None)
    args = ap.parse_args()

    spark = SparkSession.builder.appName(f"t24-parse-{args.table}").getOrCreate()

    # đọc SS qua Spark (handle s3a://) — nhất quán với streaming, không dùng open()
    ss_rows = spark.read.option("multiLine", "true").json(args.ss).collect()
    ss = {r["RECID"]: r["XMLRECORD"] for r in ss_rows}
    spec = build_spec(args.table, ss)
    frames = parse(read_raw(spark, args.input), spec)

    if args.business_date:
        n = write_bronze_pull(frames, args.target, args.business_date, dlq_path=args.dlq)
        print(f"[pull] wrote {n} rows to {args.target} business_date={args.business_date}")
    else:
        write_bronze(frames, args.target, dlq_path=args.dlq)

    spark.stop()


if __name__ == "__main__":
    main()
