"""
PySpark Script: Treasury External Liquidity Parser
────────────────────────────────────────────────────
Source  : MinIO  s3a://raw/treasury_external/<YYYY-MM-DD>/liqui_treasury_external_<YYYY-MM-DD>.xlsx
Output  : Iceberg hive.bronze.treasury_external (partitioned by report_date)
Writer  : overwritePartitions (idempotent)

File structure (flat, 1 header row + data rows):
  BRANCH | DepWithBCTL | DepWithBNU | DepWithMandiri | DepWithOthers

Date extraction: regex \d{4}-\d{2}-\d{2} from filename.

Usage:
    python treasury_external_parser_spark.py --date YYYY-MM-DD
"""

import argparse
import logging
import re
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Optional

import openpyxl
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OUTPUT_SCHEMA = StructType([
    StructField("BRANCH",         StringType(), True),
    StructField("DepWithBCTL",    DoubleType(), True),
    StructField("DepWithBNU",     DoubleType(), True),
    StructField("DepWithMandiri", DoubleType(), True),
    StructField("DepWithOthers",  DoubleType(), True),
    StructField("report_date",    StringType(), True),
])


def extract_date_from_filename(filename: str) -> Optional[str]:
    """Extract YYYY-MM-DD from filename like liqui_treasury_external_2025-01-23.xlsx."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    return m.group(1) if m else None


def _to_float(value) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def parse_excel_bytes(wb_bytes: bytes, filename: str) -> List[Dict]:
    """
    Parse xlsx bytes into list of row dicts.
    Expects row 1 = header, rows 2+ = data.
    """
    report_date = extract_date_from_filename(filename)
    if not report_date:
        logger.warning(f"Cannot extract date from {filename!r}, skipping file")
        return []

    wb = openpyxl.load_workbook(BytesIO(wb_bytes), read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if not rows:
        logger.warning(f"Empty workbook in {filename!r}")
        return []

    header = [str(h).strip() if h is not None else f"col_{i}" for i, h in enumerate(rows[0])]
    expected = {"BRANCH", "DepWithBCTL", "DepWithBNU", "DepWithMandiri", "DepWithOthers"}
    if not expected.issubset(set(header)):
        raise ValueError(f"Unexpected header in {filename!r}: {header}")

    output_rows = []
    for row in rows[1:]:
        if not any(row):
            continue
        d = dict(zip(header, row))
        output_rows.append({
            "BRANCH":         str(d["BRANCH"]).strip() if d.get("BRANCH") else None,
            "DepWithBCTL":    _to_float(d.get("DepWithBCTL")),
            "DepWithBNU":     _to_float(d.get("DepWithBNU")),
            "DepWithMandiri": _to_float(d.get("DepWithMandiri")),
            "DepWithOthers":  _to_float(d.get("DepWithOthers")),
            "report_date":    report_date,
        })

    return output_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",   required=True, help="COB date YYYY-MM-DD")
    parser.add_argument("--bucket", default="raw", help="MinIO bucket for raw files")
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid --date: {args.date!r}. Expected YYYY-MM-DD")

    process_date = args.date
    input_path   = f"s3a://{args.bucket}/treasury_external/{process_date}/*.xlsx"
    table_name   = "hive.bronze.treasury_external"

    spark = SparkSession.builder \
        .appName(f"treasury-external-parser-{process_date}") \
        .getOrCreate()

    logger.info("=== TREASURY EXTERNAL LIQUIDITY PARSER ===")
    logger.info(f"Date       : {process_date}")
    logger.info(f"Input path : {input_path}")
    logger.info(f"Target     : {table_name}")

    try:
        binary_rdd = spark.sparkContext.binaryFiles(input_path, minPartitions=1)

        if binary_rdd.isEmpty():
            raise RuntimeError(
                f"No xlsx files found at {input_path}. "
                f"Verify the file was synced to MinIO for date {process_date}."
            )

        all_rows = []  # type: List[Dict]
        for file_path, file_bytes in binary_rdd.collect():
            filename = file_path.split("/")[-1]
            logger.info(f"Parsing: {file_path}")
            parsed = parse_excel_bytes(file_bytes, filename)
            logger.info(f"  -> {len(parsed)} rows parsed")
            all_rows.extend(parsed)

        if not all_rows:
            raise RuntimeError(
                f"Parsed 0 rows for date {process_date}. File may be empty or malformed."
            )

        logger.info(f"Total rows: {len(all_rows)}")
        df = spark.createDataFrame(all_rows, schema=OUTPUT_SCHEMA)

        write_props = {
            "format-version":                           "2",
            "write.metadata.delete-after-commit.enabled": "true",
            "write.target-file-size-bytes":             "268435456",
            "write.parquet.compression-codec":          "zstd",
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
