"""
PySpark Script: Treasury Incoming Remittance Parser
────────────────────────────────────────────────────
Source  : MinIO  s3a://raw/treasury/<YYYY-MM-DD>/<name>.xlsx
Output  : Iceberg hive.bronze.treasury_remittance (partitioned by report_date)
Writer  : overwritePartitions (idempotent)

Transformation rules:
- Read Excel files from MinIO
- Parse transactions with 20-column output schema
- Write to Iceberg with partition by report_date (YYYY-MM-DD)
- Auto-handle date extraction from filenames (format: DD.MM.YYYY)

Usage:
    python treasury_remittance_parser_spark.py --date YYYY-MM-DD
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, lit, when, regexp_extract, trim, explode, split, 
    array_contains, lower, coalesce, input_file_name, floor, unix_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
import logging
import re
from datetime import datetime
import openpyxl
from io import BytesIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════════════

OUTPUT_SCHEMA = StructType([
    StructField("NO", IntegerType(), True),
    StructField("DATE_TRANSFER", StringType(), True),
    StructField("TRANSACTION_REF", StringType(), True),
    StructField("REF_NUMBER", StringType(), True),
    StructField("ORDERING_CUSTOMER", StringType(), True),
    StructField("ORDERING_CUSTOMER_ACC_NO", StringType(), True),
    StructField("SENDER_BIC", StringType(), True),
    StructField("ORIGIN_OF_COUNTRY", StringType(), True),
    StructField("DEBIT_ACCOUNT", StringType(), True),
    StructField("BENEFICIARY", StringType(), True),
    StructField("GENDER", StringType(), True),
    StructField("CREDIT_ACCOUNT", StringType(), True),
    StructField("DEBIT_CCY", StringType(), True),
    StructField("DEBIT_AMOUNT", DoubleType(), True),
    StructField("CHARGE_CCY", StringType(), True),
    StructField("CHARGES", DoubleType(), True),
    StructField("CREDIT_CCY", StringType(), True),
    StructField("CREDIT_AMOUNT", DoubleType(), True),
    StructField("RATE_BNCTL", StringType(), True),
    StructField("REMARK", StringType(), True),
    StructField("REMITTANCE_INFORMATION", StringType(), True),
    StructField("report_date", StringType(), True),
])

# ═══════════════════════════════════════════════════════════════════════════════
# Transformation Logic (converted from Python parser)
# ═══════════════════════════════════════════════════════════════════════════════

def fmt_date(value) -> str:
    """int YYYYMMDD → 'DD-Mon-YY'  e.g. 20260126 → '26-Jan-26'"""
    if value is None:
        return ""
    try:
        s = str(int(value))
        dt = datetime.strptime(s, "%Y%m%d")
        return dt.strftime("%d-%b-%y")
    except:
        return ""

def strip_line_prefix(text: str) -> str:
    """Remove leading 'N/' numbering from T24 multi-line fields."""
    if not text:
        return ""
    return re.sub(r"^\d+/", "", str(text)).strip()

def origin_from_continuations(cont_lines: list) -> str:
    """
    If any continuation line starts with '3/' → extract segment between
    first and second slash (the country code).
    Otherwise → return the last non-empty line as-is.
    """
    if not cont_lines:
        return ""
    for line in cont_lines:
        if line and str(line).startswith("3/"):
            parts = str(line).split("/")
            return parts[1] if len(parts) > 1 else ""
    non_empty = [str(l) for l in cont_lines if l and str(l).strip()]
    return non_empty[-1] if non_empty else ""

def gender_from_beneficiary(ben: str) -> str:
    """Infer gender from title in beneficiary name"""
    if not ben:
        return ""
    if re.search(r"\bMrs\.?\b|\bMs\.?\b|\bMiss\.?\b", ben, re.I):
        return "Female"
    if re.search(r"\bMr\.?\b", ben, re.I):
        return "Male"
    return ""

def extract_date_from_filename(filename: str) -> str:
    """
    Extract date from filename (format: DD.MM.YYYY)
    Returns: YYYY-MM-DD or None
    """
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", filename)
    if m:
        dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
        return f"{yyyy}-{mm}-{dd}"
    return None

# ═══════════════════════════════════════════════════════════════════════════════
# Parse Excel File (Python-based on local read)
# ═══════════════════════════════════════════════════════════════════════════════

def parse_excel_bytes(wb_bytes: bytes, filename: str) -> list:
    """
    Read Excel bytes and return list of parsed row dicts
    
    Input structure (from T24):
    - Col 0: TRANSACTION REF
    - Col 1: REF NUMBER
    - Col 2: ORDERING CUSTOMER (with continuation)
    - Col 3: ORDERING CUST ACCOUNT
    - ... etc
    
    Returns list of dicts with all 21 columns
    """
    report_date = extract_date_from_filename(filename)
    if not report_date:
        logger.warning(f"Cannot extract date from {filename}, skipping")
        return []

    wb = openpyxl.load_workbook(BytesIO(wb_bytes), data_only=True)
    ws = wb.active

    # Collect (main_row, [continuation values]) groups
    groups = []
    current_main = None
    current_cont = []

    for row_idx, row in enumerate(ws.iter_rows(min_row=3, values_only=True)):
        txn_ref = row[0]
        ordering_cust_cell = row[2]

        if txn_ref is not None:
            if current_main is not None:
                groups.append((current_main, current_cont))
            current_main = row
            current_cont = []
        else:
            current_cont.append(ordering_cust_cell)

    if current_main is not None:
        groups.append((current_main, current_cont))

    output_rows = []
    for seq, (main, cont) in enumerate(groups, start=1):
        # Unpack main row (17 columns expected)
        txn_ref = main[0]
        ref_num = main[1]
        ordering_cust = main[2]
        ord_acc = main[3]
        sender_bic = main[4]
        value_date = main[5]  # int YYYYMMDD
        debit_acc = main[6]
        received_from = main[7]  # not used directly
        credit_acc = main[8]
        beneficiary = main[9]
        debit_ccy = main[10]
        debit_amt = main[11]
        charge_ccy = main[12]
        charges = main[13]
        credit_ccy = main[14]
        credit_amt = main[15]
        ft_link = main[16]

        ben_str = str(beneficiary) if beneficiary else ""
        cust_str = str(ordering_cust) if ordering_cust else ""

        output_rows.append({
            "NO": seq,
            "DATE_TRANSFER": fmt_date(value_date),
            "TRANSACTION_REF": str(txn_ref) if txn_ref else "",
            "REF_NUMBER": str(ref_num) if ref_num is not None else "",
            "ORDERING_CUSTOMER": strip_line_prefix(cust_str),
            "ORDERING_CUSTOMER_ACC_NO": str(ord_acc).strip() if ord_acc is not None else "",
            "SENDER_BIC": str(sender_bic) if sender_bic else "",
            "ORIGIN_OF_COUNTRY": origin_from_continuations(cont),
            "DEBIT_ACCOUNT": str(debit_acc) if debit_acc is not None else "",
            "BENEFICIARY": ben_str,
            "GENDER": gender_from_beneficiary(ben_str),
            "CREDIT_ACCOUNT": str(credit_acc) if credit_acc is not None else "",
            "DEBIT_CCY": str(debit_ccy) if debit_ccy else "",
            "DEBIT_AMOUNT": float(debit_amt) if debit_amt is not None else None,
            "CHARGE_CCY": str(charge_ccy) if charge_ccy else "",
            "CHARGES": float(charges) if charges is not None else None,
            "CREDIT_CCY": str(credit_ccy) if credit_ccy else "",
            "CREDIT_AMOUNT": float(credit_amt) if credit_amt is not None else None,
            "RATE_BNCTL": "",
            "REMARK": str(ft_link) if ft_link else "",
            "REMITTANCE_INFORMATION": "",
            "report_date": report_date,
        })

    return output_rows

# ═══════════════════════════════════════════════════════════════════════════════
# Main Spark Processing
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    from datetime import datetime

    parser = argparse.ArgumentParser()
    parser.add_argument('--date', required=True, help="YYYY-MM-DD (COB)")
    args = parser.parse_args()

    # Validate format
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid --date format: {args.date}. Expected YYYY-MM-DD")

    process_date = args.date

    spark = SparkSession.builder \
        .appName(f"treasury-remittance-parser-{process_date}") \
        .getOrCreate()

    logger.info("=== TREASURY REMITTANCE PARSER (Spark) ===")
    logger.info(f"Processing Treasury Remittance for date: {process_date}")

    input_prefix = "s3a://raw/treasury"
    target_path = f"{input_prefix}/{process_date}/*.xlsx"
    
    logger.info(f"Reading Excel files from: {target_path}")
    
    try:
        # Use Spark binaryFiles to read all Excel files in the date folder
        # Returns (filepath, binary_content) tuples
        binary_rdd = spark.sparkContext.binaryFiles(target_path, minPartitions=1)
        
        if binary_rdd.isEmpty():
            logger.warning(f"No Excel files found at {target_path}")
            return
        
        all_rows = []
        
        # Process each file
        for file_path, file_content in binary_rdd.collect():
            filename = file_path.split('/')[-1]
            logger.info(f"Processing: {file_path}")
            
            try:
                # Parse Excel directly from bytes
                parsed_rows = parse_excel_bytes(file_content, filename)
                all_rows.extend(parsed_rows)
                logger.info(f"  → Parsed {len(parsed_rows)} transactions")
            except Exception as e:
                logger.error(f"Error parsing {filename}: {e}")
                raise

        if not all_rows:
            logger.warning("No transactions found in date range")
            return

        # Convert to DataFrame
        logger.info(f"Total transactions parsed: {len(all_rows)}")
        df = spark.createDataFrame(all_rows, schema=OUTPUT_SCHEMA)

        # Write to Iceberg table
        # Use createOrReplace to auto-create table if not exists (first run)
        # Then overwritePartitions on subsequent runs per COB
        table_name = "hive.bronze.treasury_remittance"
        logger.info(f"Writing to Iceberg table: {table_name}")
        
        try:
            # Try overwrite partitions first (table exists)
            df.writeTo(table_name) \
                .tableProperty("format-version", "2") \
                .tableProperty("write.metadata.delete-after-commit.enabled", "true") \
                .tableProperty("write.target-file-size-bytes", "268435456") \
                .tableProperty("write.parquet.compression-codec", "zstd") \
                .overwritePartitions()
        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "cannot be found" in str(e):
                # Table doesn't exist, create it
                logger.info(f"Table {table_name} does not exist, creating...")
                df.writeTo(table_name) \
                    .tableProperty("format-version", "2") \
                    .tableProperty("write.metadata.delete-after-commit.enabled", "true") \
                    .tableProperty("write.target-file-size-bytes", "268435456") \
                    .tableProperty("write.parquet.compression-codec", "zstd") \
                    .partitionedBy("report_date") \
                    .create()
            else:
                raise
        
        logger.info("✅ Treasury Remittance parsing complete")

    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
