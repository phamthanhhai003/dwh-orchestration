"""
PySpark Script to process CREDIT_SOURCE data files
Converts 3 standalone pandas/boto3 scripts into a single PySpark + Iceberg processor
Optimized for MinIO/S3 on Kubernetes with Robust Date Parsing & Iceberg write

Tables written:
  hive.bronze.credit__customer_credit         → partitioned by partition_date (DateType)
  hive.bronze.credit__loan_master_credit        → partitioned by partition_date (DateType)
  hive.bronze.credit__loan_provision_credit     → partitioned by partition_date (DateType)
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, coalesce, lit, trim, when, year, expr
)
from functools import reduce
import logging
import re
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# BỘ LỌC TÊN CỘT: Cứu tinh cho Dremio & Hive Metastore
# ══════════════════════════════════════════════════════════════════════════════
def sanitize_column_names(columns):
    seen = {}
    new_cols = []
    for c in columns:
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', c)
        clean_name = re.sub(r'_+', '_', clean_name)
        clean_name = clean_name.strip('_').lower()
        clean_name = clean_name[:100]
        if not clean_name: clean_name = "unknown_col"
        if clean_name in seen:
            seen[clean_name] += 1
            clean_name = f"{clean_name}_{seen[clean_name]}"
        else:
            seen[clean_name] = 0
        new_cols.append(clean_name)
    return new_cols

# ══════════════════════════════════════════════════════════════════════════════
# HELPER: Ghi Iceberg
# ══════════════════════════════════════════════════════════════════════════════
def write_to_iceberg(df, table_name, partition_cols, spark, logger_ref):
    flat_name = table_name.replace(".", "__")
    full_name = f"hive.bronze.{flat_name}"

    base = (
        df.writeTo(full_name)
        .tableProperty("format-version",                      "2")
        .tableProperty("write.metadata.delete-after-commit.enabled",  "true")
        .tableProperty("write.metadata.previous-versions-max",        "10")
        .tableProperty("write.target-file-size-bytes",                "268435456")
        .tableProperty("write.parquet.row-group-size-bytes",          "134217728")
        .tableProperty("write.parquet.compression-codec",             "zstd")
    )

    table_exists = False
    try:
        table_exists = spark.catalog.tableExists(full_name)
    except Exception as e:
        err = str(e)
        if "NotFoundException" in err or "Location does not exist" in err:
            logger_ref(f"    ⚠️  Metadata mất trên MinIO, drop HMS record: {full_name}")
            try:
                spark.sql(f"DROP TABLE IF EXISTS {full_name}")
                logger_ref(f"    ✅ Drop HMS record thành công")
            except Exception as drop_err:
                logger_ref(f"    ❌ Drop thất bại: {str(drop_err)}")
                raise
            table_exists = False
        else:
            raise

    if not table_exists:
        logger_ref(f"    ✨ Tạo bảng Iceberg mới: {full_name}")
        base.partitionedBy(*partition_cols).create()
    else:
        logger_ref(f"    🔄 overwritePartitions: {full_name}")
        base.overwritePartitions()


class CreditSourceProcessor:
    def __init__(self, spark, process_date: str):
        self.spark = spark
        self.process_date = process_date
        self.base_path = f"s3a://raw/credit/{process_date}"
        self.log_info(f"Initialized Credit Source Processor for COB {process_date}")
        self.log_info(f"Input path: {self.base_path}")

    def log_info(self, message):
        logger.info(f"[CREDIT_SOURCE] {message}")

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

    # ══════════════════════════════════════════════════════════════════════════
    # 1. CUSTOMER CREDIT
    # ══════════════════════════════════════════════════════════════════════════
    CUSTOMER_HEADERS = [
        "Customer ID", "Company Book", "Account Officer", "Officer Name", "Customer Type",
        "Name 1", "Short Name", "Full Name", "Gender", "Marital Status",
        "Language", "Nationality", "Residence", "Province/State",
        "City/Municipal", "Suburb/Town", "Industry", "Industry Desc",
        "Sector", "Sector Desc", "Opening Date", "Birth Date", "Age",
        "Employ Status", "Profession", "Salary Range", "Business Type",
        "Classification", "Cust Status", "Blocked", "Date.Time", "Legal.Id",
        "Id.Types", "Bnctl.Pep", "Occupation", "Sp.Name", "Telmobile",
        "Employ.Name", "Alter Id No", "Alter Id Type", "Street",
        "Legal.Id_2", "Legal.Doc.Name", "Date of Birth", "Tel.Mobile",
        "Phone.1", "Sms.1"
    ]

    def process_customer_credit(self):
        self.log_info("Processing Customer Credit CSV files...")
        try:
            pattern   = f"{self.base_path}/[Cc][Uu][Ss][Tt][Oo][Mm][Ee][Rr]*.csv"
            file_list = self.find_files_with_pattern(pattern)
            if not file_list: return

            # Tự động làm sạch toàn bộ Headers
            clean_headers = sanitize_column_names(self.CUSTOMER_HEADERS)
            all_dfs = []
            partition_date = self.process_date

            for input_path in file_list:
                self.log_info(f"  Reading: {input_path}")

                df = self.spark.read \
                    .option("header",    "false") \
                    .option("sep",       "|") \
                    .option("multiLine", "true") \
                    .option("escape",    '"') \
                    .csv(input_path)

                num_data_cols = len(df.columns)
                num_headers   = len(clean_headers)

                if num_data_cols < num_headers:
                    df = df.toDF(*clean_headers[:num_data_cols])
                elif num_data_cols > num_headers:
                    extra = [f"extra_col_{i}" for i in range(num_headers + 1, num_data_cols + 1)]
                    df = df.toDF(*(clean_headers + extra))
                else:
                    df = df.toDF(*clean_headers)

                df = df.withColumn("partition_date", to_date(lit(partition_date)))
                all_dfs.append(df)

            if not all_dfs: return

            # Gộp và Gom file (Repartition) trước khi ghi
            combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), all_dfs)
            self.log_info("  📦 Gom dữ liệu (Repartition) để tối ưu hóa Dremio...")
            combined = combined.repartition(10)
            
            write_to_iceberg(combined, "credit.customer_credit", ["partition_date"], self.spark, self.log_info)
            self.log_info(f"  ✓ Processed {len(all_dfs)} Customer Credit files.")
        except Exception as e:
            self.log_info(f"  ✗ Error: {str(e)}")
            raise

    # ══════════════════════════════════════════════════════════════════════════
    # 2. LOAN MASTER CREDIT  (CRSI Report)
    # ══════════════════════════════════════════════════════════════════════════
    LOAN_MASTER_HEADERS = [
        "Loan ID", "Category", "Product Name", "Customer Number",
        "Account Officer", "Currency", "Auto Class", "Loan Outstanding", "Provision Type",
        "Base Amount", "Commitment", "Term", "Maturity Date", "Arrangement Status",
        "Repayment Amount", "Last Date Cr", "Opening Date", "ID Type", "Customer Name",
        "Co Code", "Arrangement ID", "Legal ID", "Employer Name", "Interest Rate",
        "Current Interest", "Avail Principal", "Occupation", "Customer Service Provider",
        "Customer Telephone mobile", "Customer contact name", "Customer sign telephone number",
        "Customer sign occupation"
    ]

    def process_loan_master_credit(self):
        self.log_info("Processing Loan Master Credit (CRSI Report) files...")
        try:
            pattern   = f"{self.base_path}/[Bb]nctl.[Cc]rsi[Rr]eport.*.csv"
            file_list = self.find_files_with_pattern(pattern)
            if not file_list: return

            clean_headers = sanitize_column_names(self.LOAN_MASTER_HEADERS)
            all_dfs = []
            partition_date = self.process_date

            for input_path in file_list:
                self.log_info(f"  Reading: {input_path}")

                df = self.spark.read \
                    .option("header",    "false") \
                    .option("sep",       "|") \
                    .option("multiLine", "true") \
                    .option("escape",    '"') \
                    .csv(input_path)

                raw_cols = df.columns
                df = df.select(raw_cols[1:]) # drop _c0

                num_data_cols = len(df.columns)
                num_headers   = len(clean_headers)

                if num_data_cols < num_headers:
                    df = df.toDF(*clean_headers[:num_data_cols])
                elif num_data_cols > num_headers:
                    extra = [f"extra_col_{i}" for i in range(num_headers + 1, num_data_cols + 1)]
                    df = df.toDF(*(clean_headers + extra))
                else:
                    df = df.toDF(*clean_headers)

                df = df.withColumn("partition_date", to_date(lit(partition_date)))
                all_dfs.append(df)

            if not all_dfs: return

            combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), all_dfs)
            self.log_info("  📦 Gom dữ liệu (Repartition) để tối ưu hóa Dremio...")
            combined = combined.repartition(10)
            
            write_to_iceberg(combined, "credit.loan_master_credit", ["partition_date"], self.spark, self.log_info)
            self.log_info(f"  ✓ Processed {len(all_dfs)} Loan Master Credit files.")
        except Exception as e:
            self.log_info(f"  ✗ Error: {str(e)}")
            raise

    # ══════════════════════════════════════════════════════════════════════════
    # 3. LOAN PROVISION CREDIT
    # ══════════════════════════════════════════════════════════════════════════
    PROVISION_COLUMN_MAP = {
        "Account Number"      : 1, 
        "Category"            : 2, 
        "Product Name"        : 3,
        "Customer Number"     : 4,
        "Customer Name"       : 6, 
        "Account Officer"     : 5, 
        "Currency"            : 7,
        "Auto Class"          : 8, 
        "Loan Outstanding"    : 9, 
        "Posted Provision"    : 10,
        "Provision Type"      : 11,
        "Base Amount"         : 12,
        "Calculated Provision": 13,
        "Manual Provision"    : 14,
    }

    def process_loan_provision_credit(self):
        self.log_info("Processing Loan Provision Credit CSV files...")
        try:
            pattern   = f"{self.base_path}/[Bb]nctl.[Ll]oan.[Pp]rovision.*.csv"
            file_list = self.find_files_with_pattern(pattern)
            if not file_list: return

            all_dfs = []
            partition_date = self.process_date

            for input_path in file_list:
                self.log_info(f"  Reading: {input_path}")

                df = self.spark.read \
                    .option("header",    "false") \
                    .option("sep",       "|") \
                    .option("multiLine", "true") \
                    .option("escape",    '"') \
                    .csv(input_path)

                raw_cols = df.columns
                select_exprs = []
                for out_name, raw_idx in self.PROVISION_COLUMN_MAP.items():
                    # Làm sạch tên alias cho từng cột
                    clean_out_name = sanitize_column_names([out_name])[0]
                    
                    if raw_idx < len(raw_cols):
                        select_exprs.append(col(raw_cols[raw_idx]).alias(clean_out_name))
                    else:
                        select_exprs.append(lit(None).cast("string").alias(clean_out_name))

                df = df.select(*select_exprs)

                # Filter dùng tên cột ĐÃ làm sạch
                clean_account_col = sanitize_column_names(["Account Number"])[0]
                df = df.filter(
                    col(clean_account_col).isNotNull() &
                    (trim(col(clean_account_col)) != "")
                )

                df = df.withColumn("partition_date", to_date(lit(partition_date)))
                all_dfs.append(df)

            if not all_dfs: return

            combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), all_dfs)
            self.log_info("  📦 Gom dữ liệu (Repartition) để tối ưu hóa Dremio...")
            combined = combined.repartition(10)
            
            write_to_iceberg(combined, "credit.loan_provision_credit", ["partition_date"], self.spark, self.log_info)
            self.log_info(f"  ✓ Processed {len(all_dfs)} Loan Provision Credit files.")
        except Exception as e:
            self.log_info(f"  ✗ Error: {str(e)}")
            raise

    def process_all(self):
        self.log_info("=" * 80)
        self.log_info("Starting CREDIT_SOURCE data processing from MinIO")
        self.log_info("=" * 80)
        try:
            self.log_info("\n--- Credit Section ---")
            self.process_customer_credit()
            self.process_loan_master_credit()
            self.process_loan_provision_credit()

            self.log_info("\n" + "=" * 80)
            self.log_info("✓ All Credit files processed successfully!")
            self.log_info("=" * 80)
        except Exception as e:
            self.log_info(f"\n✗ Error during processing: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', required=True, help="YYYY-MM-DD (COB)")
    args = parser.parse_args()

    if not re.match(r'^\d{4}-\d{2}-\d{2}$', args.date):
        raise ValueError(f"Invalid --date format: {args.date}. Expected YYYY-MM-DD")

    spark = SparkSession.builder \
        .appName(f"CreditSourceProcessor-{args.date}") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    try:
        processor = CreditSourceProcessor(spark, args.date)
        processor.process_all()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()