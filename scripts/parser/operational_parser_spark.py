"""
PySpark Script to process OPERATION_SOURCE data files
Optimized for MinIO/S3 on Kubernetes with Robust Date Parsing & Iceberg write
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, coalesce, lit, substring, regexp_replace, trim, when, year, expr
)
from pyspark.sql.types import StringType
from functools import reduce
import logging
import re
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# HELPER: Ghi Iceberg — dùng chung cho tất cả 8 hàm
#
# Iceberg identifier chỉ chấp nhận đúng 3 phần: catalog.database.table
# table_name truyền vào dạng "digital_service.atm_local_transactions"
# → flatten dấu . thành __ trước khi ghép vào full identifier
# → hive.bronze.digital_service__atm_local_transactions  ✅
# → hive.bronze.digital_service.atm_local_transactions   ❌ (4 phần → invalid)
# ══════════════════════════════════════════════════════════════════════════════
def write_to_iceberg(df, table_name, partition_cols, spark, logger_ref):
    flat_name = table_name.replace(".", "__")
    full_name = f"hive.bronze.{flat_name}"

    base = (
        df.writeTo(full_name)
        .tableProperty("format-version",                              "2")
        .tableProperty("write.metadata.delete-after-commit.enabled",  "true")
        .tableProperty("write.metadata.previous-versions-max",        "10")
        .tableProperty("write.target-file-size-bytes",                "268435456")
        .tableProperty("write.parquet.row-group-size-bytes",          "134217728")
        .tableProperty("write.parquet.compression-codec",             "zstd")
    )

    # ── Kiểm tra bảng tồn tại — có thể throw nếu metadata bị xóa khỏi MinIO
    table_exists = False
    try:
        table_exists = spark.catalog.tableExists(full_name)
    except Exception as e:
        err = str(e)
        if "NotFoundException" in err or "Location does not exist" in err:
            # HMS còn record nhưng MinIO đã mất file → coi như bảng chưa tồn tại
            # Cần drop khỏi HMS trước khi tạo lại
            logger_ref(f"    ⚠️  Metadata mất trên MinIO, drop HMS record: {full_name}")
            try:
                spark.sql(f"DROP TABLE IF EXISTS {full_name}")
                logger_ref(f"    ✅ Drop HMS record thành công")
            except Exception as drop_err:
                logger_ref(f"    ❌ Drop thất bại: {str(drop_err)}")
                raise
            table_exists = False
        else:
            # Lỗi khác thì re-raise
            raise

    if not table_exists:
        logger_ref(f"    ✨ Tạo bảng Iceberg mới: {full_name}")
        base.partitionedBy(*partition_cols).create()
    else:
        logger_ref(f"    🔄 overwritePartitions: {full_name}")
        base.overwritePartitions()


class OperationSourceProcessor:
    def __init__(self, spark):
        import os
        self.spark = spark
        # Read PROCESS_MONTH from env (e.g., "2026-04"). REQUIRED — không fallback "process all".
        process_month = os.getenv("PROCESS_MONTH", "").strip()
        if not process_month:
            raise ValueError(
                "PROCESS_MONTH env var is required (format YYYY-MM). "
                "Không chấp nhận fallback 'process all months' vì sẽ gây race condition "
                "và overwrite sai partition."
            )
        if not re.match(r'^\d{4}-\d{2}$', process_month):
            raise ValueError(
                f"Invalid PROCESS_MONTH format: {process_month}. Expected YYYY-MM."
            )
        self.process_month = process_month
        self.base_path = f"s3a://raw/operation/{process_month}"
        self.log_info(f"Processing month: {process_month}")
        self.log_info(f"Base path: {self.base_path}")

    def log_info(self, message):
        logger.info(f"[OPERATION_SOURCE] {message}")

    # ── Giữ nguyên 100% — JVM Hadoop glob API ────────────────────────────────
    def find_files_with_pattern(self, pattern):
        try:
            sc = self.spark.sparkContext
            Path_jvm    = sc._gateway.jvm.org.apache.hadoop.fs.Path
            hadoop_conf = sc._jsc.hadoopConfiguration()
            path_obj    = Path_jvm(pattern)
            fs          = path_obj.getFileSystem(hadoop_conf)
            file_statuses = fs.globStatus(path_obj)
            if file_statuses:
                return sorted([f.getPath().toString() for f in file_statuses])
            return []
        except Exception as e:
            self.log_info(f"⚠ Error finding files with pattern {pattern}: {str(e)}")
            return []

    # ── Giữ nguyên 100% — business logic parse date ──────────────────────────
    def parse_date_robustly(self, df, column_name, fallback_date_str):
        formats = [
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "yyyy-MM-dd'T'HH:mm:ss.SSS",     "yyyy-MM-dd'T'HH:mm:ss",
            "MM/dd/yyyy hh:mm:ss a",          "M/d/yyyy h:mm:ss a",
            "dd/MM/yyyy hh:mm:ss a",          "d/M/yyyy h:mm:ss a",
            "yyyy-MM-dd HH:mm:ss.S",          "yyyy-MM-dd HH:mm:ss",
            "dd/MM/yyyy HH:mm:ss",            "dd-MM-yyyy HH:mm:ss",
            "M/d/yy  HH:mm",                  "MM/dd/yy  HH:mm",
            "M/d/yy HH:mm",                   "MM/dd/yy HH:mm",
            "d/M/yy HH:mm",                   "dd/MM/yy HH:mm",
            "dd-MM-yyyy", "dd/MM/yyyy", "yyyy-MM-dd", "MM/dd/yyyy",
            "d-M-yyyy",   "d/M/yyyy",   "M/d/yy",     "d/M/yy",
            "dd/MM/yy",   "MM/dd/yy"
        ]
        date_exprs = [to_date(col(column_name), f) for f in formats]

        df = df.withColumn("tmp_date",
                           coalesce(coalesce(*date_exprs),
                                    to_date(lit(fallback_date_str))))
        df = df.withColumn(
            "transaction_date",
            when(
                year("tmp_date") < 100,
                expr("make_date(year(tmp_date) + 2000, month(tmp_date), day(tmp_date))")
            ).otherwise(col("tmp_date"))
        ).drop("tmp_date")
        return df

    # ══════════════════════════════════════════════════════════════════════════
    # HNF Section
    # ══════════════════════════════════════════════════════════════════════════

    def process_account_customer(self):
        self.log_info("Processing AccountCustomer CSV files...")
        try:
            pattern   = f"{self.base_path}/hnf/AccountCustomer.*.csv"
            file_list = self.find_files_with_pattern(pattern)
            if not file_list:
                return

            schema_header = [
                "ACCOUNT", "ACCOUNT_ID", "PRODCAT", "CID", "ACC_OPENED_DATE",
                "DOB", "NAME", "SHORT_NAME", "OTHER_NAME", "AMOUNT", "COMPANY", "GENDER"
            ]

            all_dfs = []
            for input_path in file_list:
                self.log_info(f"  Reading: {input_path}")
                filename   = Path(input_path).name
                date_match = re.search(r'(\d{8})', filename)
                if not date_match:
                    continue

                date_str       = date_match.group(1)
                partition_date = f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"

                df = self.spark.read \
                    .option("header", "false") \
                    .option("sep", "|") \
                    .csv(input_path)

                # toDF() 1 lần thay vì loop withColumnRenamed
                if len(df.columns) == len(schema_header):
                    df = df.toDF(*schema_header)

                # FIX: cast partition_date to DateType instead of keeping as string literal
                df = df.withColumn("partition_date", to_date(lit(partition_date)))
                all_dfs.append(df)

            if not all_dfs:
                return

            combined = reduce(lambda a, b: a.union(b), all_dfs)
            write_to_iceberg(
                combined,
                "hnf.account_customer",
                # FIX: removed redundant partition_month, use only partition_date
                ["partition_date"],
                self.spark,
                self.log_info
            )
            self.log_info(f"  ✓ Processed {len(all_dfs)} AccountCustomer files.")
        except Exception as e:
            self.log_info(f"  ✗ Error: {str(e)}")

    def process_crb_sheet4(self):
        self.log_info("Processing CRB Excel files (Sheet4)...")
        try:
            pattern   = f"{self.base_path}/hnf/*_CRB.xlsx"
            file_list = self.find_files_with_pattern(pattern)
            if not file_list:
                return

            rename_map = {
                0:  "LINE",         1: "CURRENCY",     2: "CUSTOMER",
                3:  "CURRENCY_AMT", 4: "LOCAL_CCY_AMT", 5: "INT_RATE",
                6:  "INT",          7: "VALUE_DATE",    8: "MAT_DATE",
                9:  "BRANCH",       10: "PRODCAT",      13: "BRANCH_NAME"
            }

            all_dfs = []
            for input_path in file_list:
                self.log_info(f"  Reading: {input_path}")
                filename    = Path(input_path).name
                month_match = re.search(r'(\d{4})_(\d{2})', filename)
                if not month_match:
                    continue

                # FIX: derive a first-of-month date string to use as DateType partition_date
                year_str, month_str = month_match.group(1), month_match.group(2)
                partition_date = f"{year_str}-{month_str}-01"

                df = self.spark.read \
                    .format("com.crealytics.spark.excel") \
                    .option("dataAddress", "'Sheet4'!A1") \
                    .option("header", "false") \
                    .option("maxRowsInMemory", 10000) \
                    .load(input_path)

                df = df.filter(col("_c0").isNotNull() & (trim(col("_c0")) != "LINE"))

                old_cols = df.columns
                for idx, new_name in rename_map.items():
                    if idx < len(old_cols):
                        df = df.withColumnRenamed(old_cols[idx], new_name)

                safe_line_col = col("LINE").cast("double").cast("decimal(20,0)").cast("string")
                df = df.withColumn("BRANCH_ID", substring(safe_line_col, 1, 1))
                df = df.withColumn("PRODCAT",   substring(safe_line_col, 2, 4))
                # FIX: removed partition_month; replaced with DateType partition_date (first of month)
                df = df.withColumn("partition_date", to_date(lit(partition_date)))
                all_dfs.append(df)

            if not all_dfs:
                return

            combined = reduce(lambda a, b: a.union(b), all_dfs)
            write_to_iceberg(
                combined,
                "hnf.crb",
                # FIX: removed redundant partition_month, use only partition_date
                ["partition_date"],
                self.spark,
                self.log_info
            )
            self.log_info(f"  ✓ Processed {len(all_dfs)} CRB files.")
        except Exception as e:
            self.log_info(f"  ✗ Error: {str(e)}")

    # ══════════════════════════════════════════════════════════════════════════
    # Agent Banking Section
    # ══════════════════════════════════════════════════════════════════════════

    def process_agency_banking(self):
        self.log_info("Processing Agency Banking transactions files...")
        try:
            pattern   = f"{self.base_path}/agent_banking/*_TransactionsLogReport_Agency_Banking_EOM.xlsx"
            file_list = self.find_files_with_pattern(pattern)
            if not file_list:
                return

            all_dfs = []
            for input_path in file_list:
                self.log_info(f"  Reading: {input_path}")
                filename    = Path(input_path).name
                month_match = re.search(r'(\d{4})_(\d{2})', filename)
                if not month_match:
                    continue

                year_str, month_str = month_match.group(1), month_match.group(2)
                # FIX: removed partition_month variable; fallback_date used only for parse_date_robustly
                fallback_date = f"{year_str}-{month_str}-01"

                df = self.spark.read \
                    .format("com.crealytics.spark.excel") \
                    .option("header", "true") \
                    .option("maxRowsInMemory", 10000) \
                    .load(input_path)

                if "Transaction Date" in df.columns:
                    df = self.parse_date_robustly(df, "Transaction Date", fallback_date)
                else:
                    # FIX: cast to DateType instead of keeping as string literal
                    df = df.withColumn("transaction_date", to_date(lit(fallback_date)))

                # FIX: removed partition_month column; cast partition_date to DateType
                df = df.withColumn("partition_date", col("transaction_date").cast("date"))
                all_dfs.append(df)

            if not all_dfs:
                return

            combined = reduce(lambda a, b: a.union(b), all_dfs)
            write_to_iceberg(
                combined,
                "agent_banking.transactions_log",
                # FIX: removed redundant partition_month, use only partition_date
                ["partition_date"],
                self.spark,
                self.log_info
            )
            self.log_info(f"  ✓ Processed {len(all_dfs)} Agency Banking files.")
        except Exception as e:
            self.log_info(f"  ✗ Error: {str(e)}")

    # ══════════════════════════════════════════════════════════════════════════
    # Digital Service Section
    # ══════════════════════════════════════════════════════════════════════════

    def process_atm_local_transactions(self):
        self.log_info("Processing ATM Local transactions files...")
        try:
            pattern   = f"{self.base_path}/digital_service/*_Transaction_ATM_Local_EOM.csv"
            file_list = self.find_files_with_pattern(pattern)
            if not file_list:
                return

            all_dfs = []
            for input_path in file_list:
                self.log_info(f"  Reading: {input_path}")
                filename        = Path(input_path).name
                month_match     = re.search(r'(\d{4})_(\d{2})', filename)
                # FIX: removed partition_month variable; fallback_date used only for parse_date_robustly
                fallback_date   = f"{month_match.group(1)}-{month_match.group(2)}-01" \
                                  if month_match else "1970-01-01"

                df = self.spark.read \
                    .option("header",    "true") \
                    .option("multiLine", "true") \
                    .option("escape",    '"') \
                    .csv(input_path)

                if "msgtime" in df.columns:
                    df = self.parse_date_robustly(df, "msgtime", fallback_date)
                else:
                    # FIX: cast to DateType instead of keeping as string literal
                    df = df.withColumn("transaction_date", to_date(lit(fallback_date)))

                # FIX: removed partition_month column; cast partition_date to DateType
                df = df.withColumn("partition_date", col("transaction_date").cast("date"))
                all_dfs.append(df)

            if not all_dfs:
                return

            combined = reduce(lambda a, b: a.union(b), all_dfs)
            write_to_iceberg(
                combined,
                "digital_service.atm_local_transactions",
                # FIX: removed redundant partition_month, use only partition_date
                ["partition_date"],
                self.spark,
                self.log_info
            )
            self.log_info(f"  ✓ Processed {len(all_dfs)} ATM Local files.")
        except Exception as e:
            self.log_info(f"  ✗ Error: {str(e)}")

    def process_internet_banking_transactions(self):
        self.log_info("Processing Internet Banking transactions files...")
        try:
            pattern   = f"{self.base_path}/digital_service/*_Transaction_Internet_Banking_EOM.xlsx"
            file_list = self.find_files_with_pattern(pattern)
            if not file_list:
                return

            all_dfs = []
            for input_path in file_list:
                self.log_info(f"  Reading: {input_path}")
                filename        = Path(input_path).name
                month_match     = re.search(r'(\d{4})_(\d{2})', filename)
                # FIX: removed partition_month variable; fallback_date used only for parse_date_robustly
                fallback_date   = f"{month_match.group(1)}-{month_match.group(2)}-01" \
                                  if month_match else "1970-01-01"

                df = self.spark.read \
                    .format("com.crealytics.spark.excel") \
                    .option("header", "true") \
                    .option("maxRowsInMemory", 10000) \
                    .load(input_path)

                if "transferDateTime" in df.columns:
                    df = self.parse_date_robustly(df, "transferDateTime", fallback_date)
                else:
                    # FIX: cast to DateType instead of keeping as string literal
                    df = df.withColumn("transaction_date", to_date(lit(fallback_date)))

                # FIX: removed partition_month column; cast partition_date to DateType
                df = df.withColumn("partition_date", col("transaction_date").cast("date"))
                all_dfs.append(df)

            if not all_dfs:
                return

            combined = reduce(lambda a, b: a.union(b), all_dfs)
            write_to_iceberg(
                combined,
                "digital_service.internet_banking_transactions",
                # FIX: removed redundant partition_month, use only partition_date
                ["partition_date"],
                self.spark,
                self.log_info
            )
            self.log_info(f"  ✓ Processed {len(all_dfs)} Internet Banking files.")
        except Exception as e:
            self.log_info(f"  ✗ Error: {str(e)}")

    def process_transfer_report_csv(self, report_type):
        self.log_info(f"Processing {report_type} Transfer Report files...")
        try:
            pattern   = f"{self.base_path}/digital_service/*_TransferReportBnctl_{report_type}_EOM.csv"
            file_list = self.find_files_with_pattern(pattern)
            if not file_list:
                return

            all_dfs = []
            for input_path in file_list:
                self.log_info(f"  Reading: {input_path}")
                filename        = Path(input_path).name
                month_match     = re.search(r'(\d{4})_(\d{2})', filename)
                # FIX: removed partition_month variable; fallback_date used only for parse_date_robustly
                fallback_date   = f"{month_match.group(1)}-{month_match.group(2)}-01" \
                                  if month_match else "1970-01-01"

                df = self.spark.read \
                    .option("header",    "true") \
                    .option("multiLine", "true") \
                    .option("escape",    '"') \
                    .csv(input_path)

                if "Transaction Date" in df.columns:
                    df = self.parse_date_robustly(df, "Transaction Date", fallback_date)
                else:
                    # FIX: cast to DateType instead of keeping as string literal
                    df = df.withColumn("transaction_date", to_date(lit(fallback_date)))

                # FIX: removed partition_month column; cast partition_date to DateType
                df = df.withColumn("partition_date", col("transaction_date").cast("date"))
                all_dfs.append(df)

            if not all_dfs:
                return

            combined = reduce(lambda a, b: a.union(b), all_dfs)
            write_to_iceberg(
                combined,
                f"digital_service.transfer_report_{report_type.lower()}",
                # FIX: removed redundant partition_month, use only partition_date
                ["partition_date"],
                self.spark,
                self.log_info
            )
            self.log_info(f"  ✓ Processed {len(all_dfs)} {report_type} Transfer Report files.")
        except Exception as e:
            self.log_info(f"  ✗ Error: {str(e)}")

    # ══════════════════════════════════════════════════════════════════════════
    # Main — giữ nguyên 100%
    # ══════════════════════════════════════════════════════════════════════════
    def process_all(self):
        self.log_info("=" * 80)
        self.log_info("Starting OPERATION_SOURCE data processing from MinIO")
        self.log_info("=" * 80)
        try:
            self.log_info("\n--- HNF Section ---")
            self.process_account_customer()
            self.process_crb_sheet4()

            self.log_info("\n--- Agent Banking Section ---")
            self.process_agency_banking()

            self.log_info("\n--- Digital Service Section ---")
            self.process_atm_local_transactions()
            self.process_internet_banking_transactions()
            self.process_transfer_report_csv("ATM")
            self.process_transfer_report_csv("POS")
            self.process_transfer_report_csv("WEB")

            self.log_info("\n" + "=" * 80)
            self.log_info("✓ All files processed successfully!")
            self.log_info("=" * 80)
        except Exception as e:
            self.log_info(f"\n✗ Error during processing: {str(e)}")
            raise


def main():
    spark = SparkSession.builder \
        .appName("OperationSourceProcessor") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    try:
        processor = OperationSourceProcessor(spark)
        processor.process_all()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()