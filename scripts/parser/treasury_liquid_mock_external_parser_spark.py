"""
PySpark Script: Treasury Liquid Mock External Parser
──────────────────────────────────────────────────────
Source  : MinIO  s3a://raw/treasury_liquid_mock_external/<YYYY-MM-DD>/liqui_treasury_external_<YYYY-MM-DD>.xls
Output  : Iceberg hive.bronze.treasury_liquid_mock_external (partitioned by report_date)
Writer  : overwritePartitions (idempotent)

File structure (.xls, 3-row preamble):
  Row 0: Title
  Row 1: BRANCH | Value | DepWithBCTL | DepWithBNU | DepWithMandiri | DepWithOthers
  Row 2: (aggregate totals for the 4 inter-bank deposit fields — target row)
  Row 3+: per-branch rows (ignored)

Date extraction: regex \d{4}-\d{2}-\d{2} from filename.

Usage:
    python treasury_liquid_mock_external_parser_spark.py --date YYYY-MM-DD
"""

import argparse
import logging
import re
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Optional

import xlrd
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HEADER_ROW = 1
DATA_ROW = 2
TARGET_FIELDS = ["DepWithBCTL", "DepWithBNU", "DepWithMandiri", "DepWithOthers"]

OUTPUT_SCHEMA = StructType([
    StructField("DepWithBCTL",    DoubleType(), True),
    StructField("DepWithBNU",     DoubleType(), True),
    StructField("DepWithMandiri", DoubleType(), True),
    StructField("DepWithOthers",  DoubleType(), True),
    StructField("report_date",    StringType(), True),
])


def extract_date_from_filename(filename: str) -> Optional[str]:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    return m.group(1) if m else None


def _to_float(value) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def parse_xls_bytes(file_bytes: bytes, filename: str) -> List[Dict]:
    report_date = extract_date_from_filename(filename)
    if not report_date:
        logger.warning(f"Cannot extract date from {filename!r}, skipping")
        return []

    wb = xlrd.open_workbook(file_contents=file_bytes)
    ws = wb.sheet_by_index(0)

    if ws.nrows < DATA_ROW + 1:
        logger.warning(f"File {filename!r} has fewer than {DATA_ROW + 1} rows, skipping")
        return []

    header = [str(v).strip() for v in ws.row_values(HEADER_ROW)]
    missing = [f for f in TARGET_FIELDS if f not in header]
    if missing:
        raise ValueError(f"Missing columns {missing} in {filename!r}. Header: {header}")

    data = ws.row_values(DATA_ROW)
    row = {f: _to_float(data[header.index(f)]) for f in TARGET_FIELDS}
    row["report_date"] = report_date
    return [row]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",   required=True, help="COB date YYYY-MM-DD")
    parser.add_argument("--bucket", default="raw",  help="MinIO bucket for raw files")
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid --date: {args.date!r}. Expected YYYY-MM-DD")

    process_date = args.date
    input_path   = f"s3a://{args.bucket}/treasury_liquid_mock_external/{process_date}/*.xls"
    table_name   = "hive.bronze.treasury_liquid_mock_external"

    spark = SparkSession.builder \
        .appName(f"treasury-liquid-mock-external-parser-{process_date}") \
        .getOrCreate()

    logger.info("=== TREASURY LIQUID MOCK EXTERNAL PARSER ===")
    logger.info(f"Date       : {process_date}")
    logger.info(f"Input path : {input_path}")
    logger.info(f"Target     : {table_name}")

    try:
        binary_rdd = spark.sparkContext.binaryFiles(input_path, minPartitions=1)

        if binary_rdd.isEmpty():
            raise RuntimeError(
                f"No .xls files found at {input_path}. "
                f"Verify file was uploaded to MinIO for date {process_date}."
            )

        all_rows: List[Dict] = []
        for file_path, file_bytes in binary_rdd.collect():
            filename = file_path.split("/")[-1]
            logger.info(f"Parsing: {file_path}")
            parsed = parse_xls_bytes(file_bytes, filename)
            logger.info(f"  -> {len(parsed)} rows parsed")
            all_rows.extend(parsed)

        if not all_rows:
            raise RuntimeError(f"Parsed 0 rows for date {process_date}. File may be empty or malformed.")

        logger.info(f"Total rows: {len(all_rows)}")
        df = spark.createDataFrame(all_rows, schema=OUTPUT_SCHEMA)

        write_props = {
            "format-version":                             "2",
            "write.metadata.delete-after-commit.enabled": "true",
            "write.target-file-size-bytes":               "268435456",
            "write.parquet.compression-codec":            "zstd",
        }

        try:
            logger.info(f"Overwriting partition report_date={process_date} in {table_name}")
            writer = df.writeTo(table_name)
            for k, v in write_props.items():
                writer = writer.tableProperty(k, v)
            writer.overwritePartitions()
        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "cannot be found" in str(e).lower():
                logger.info(f"Table {table_name} not found, creating...")
                writer = df.writeTo(table_name)
                for k, v in write_props.items():
                    writer = writer.tableProperty(k, v)
                writer.partitionedBy("report_date").create()
            else:
                raise

        logger.info(f"Done. Wrote {len(all_rows)} rows to {table_name} (report_date={process_date})")

    except Exception as e:
        logger.error(f"FAILED: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
