"""Pull EXTRACT: JDBC (T24 readable secondary) -> MinIO raw zone (parquet).

Chặng 1 của pull (chặng 2 = t24_batch_parse.py đọc raw parquet -> Bronze).
2 mode:
  full         : đọc cả bảng (bảng nhỏ / reference / dimension).
  incremental  : WHERE <watermark-col> > <last-value> (bảng lớn append-only;
                 watermark = RECID cho event-table, hoặc cột date trên T24 thật).

    t24_jdbc_extract.py \
        --url "jdbc:sqlserver://mssql...:1433;databaseName=testdb;encrypt=false" \
        --dbtable dbo.FBNK_FUNDS_TRANSFER --user sa --password ... \
        --output s3a://raw/t24/FBNK_FUNDS_TRANSFER/2026-06-13/ \
        --mode incremental --watermark-col RECID --last-value FT25339ZZYF6
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession

from t24_parser.sources import read_jdbc


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="JDBC url tới readable secondary")
    ap.add_argument("--dbtable", required=True, help="schema.table, vd dbo.FBNK_ACCOUNT")
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default=os.environ.get("MSSQL_PASSWORD"),
                    help="mac dinh doc tu env MSSQL_PASSWORD (KHONG truyen plaintext qua args)")
    ap.add_argument("--output", required=True, help="raw parquet path (s3a://raw/...)")
    ap.add_argument("--mode", choices=["full", "incremental"], default="full")
    ap.add_argument("--watermark-col", default="RECID")
    ap.add_argument("--last-value", default=None,
                    help="incremental: chỉ lấy watermark-col > giá trị này")
    args = ap.parse_args()
    if not args.password:
        raise SystemExit("thieu password: dat env MSSQL_PASSWORD hoac truyen --password")

    spark = SparkSession.builder.appName(f"t24-extract-{args.dbtable}").getOrCreate()

    if args.mode == "incremental":
        if not args.last_value:
            raise SystemExit("mode=incremental cần --last-value")
        # T24 DB table chỉ có RECID+XMLRECORD; watermark phải là cột thật (RECID
        # cho event-table). Subquery đẩy filter xuống MSSQL.
        src = (f"(SELECT RECID, XMLRECORD FROM {args.dbtable} "
               f"WHERE {args.watermark_col} > '{args.last_value}') AS t")
    else:
        src = args.dbtable

    df = read_jdbc(spark, args.url, src, args.user, args.password).cache()
    n = df.count()
    df.write.mode("overwrite").parquet(args.output)
    print(f"EXTRACT {args.dbtable} mode={args.mode} -> {args.output}  rows={n}")
    spark.stop()


if __name__ == "__main__":
    main()
