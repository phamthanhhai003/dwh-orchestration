"""
PySpark Script to process OPERATION_SOURCE data files
Optimized for MinIO/S3 on Kubernetes with Robust Date Parsing & Ninja Overwrite
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, coalesce, lit, substring, regexp_replace, trim, when, year, expr
)
from pyspark.sql.types import StringType
import logging
import re
from pathlib import Path

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OperationSourceProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.base_path = "s3a://raw/operation"
        self.output_path = "s3a://bronze/operation"
        
        self.log_info("Initialized MinIO/S3 Data Processor")
        
    def log_info(self, message):
        logger.info(f"[OPERATION_SOURCE] {message}")
    
    def find_files_with_pattern(self, pattern):
        try:
            sc = self.spark.sparkContext
            Path_jvm = sc._gateway.jvm.org.apache.hadoop.fs.Path
            hadoop_conf = sc._jsc.hadoopConfiguration()
            path_obj = Path_jvm(pattern)
            fs = path_obj.getFileSystem(hadoop_conf)
            file_statuses = fs.globStatus(path_obj)
            if file_statuses:
                return sorted([f.getPath().toString() for f in file_statuses])
            return []
        except Exception as e:
            self.log_info(f"⚠ Error finding files with pattern {pattern}: {str(e)}")
            return []

    def delete_path_if_exists(self, path_to_delete):
        try:
            sc = self.spark.sparkContext
            Path_jvm = sc._gateway.jvm.org.apache.hadoop.fs.Path
            hadoop_conf = sc._jsc.hadoopConfiguration()
            path_obj = Path_jvm(path_to_delete)
            fs = path_obj.getFileSystem(hadoop_conf)
            if fs.exists(path_obj):
                fs.delete(path_obj, True) 
                self.log_info(f"   [Ninja Clean] Đã dọn sạch partition cũ: {path_to_delete}")
        except Exception as e:
            self.log_info(f"   ⚠ Không thể xóa partition {path_to_delete}: {str(e)}")

    def parse_date_robustly(self, df, column_name, fallback_date_str):
        formats = [
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss",
            "MM/dd/yyyy hh:mm:ss a", "M/d/yyyy h:mm:ss a",
            "dd/MM/yyyy hh:mm:ss a", "d/M/yyyy h:mm:ss a",
            "yyyy-MM-dd HH:mm:ss.S", "yyyy-MM-dd HH:mm:ss",
            "dd/MM/yyyy HH:mm:ss", "dd-MM-yyyy HH:mm:ss",
            "M/d/yy  HH:mm", "MM/dd/yy  HH:mm",
            "M/d/yy HH:mm", "MM/dd/yy HH:mm",
            "d/M/yy HH:mm", "dd/MM/yy HH:mm",
            "dd-MM-yyyy", "dd/MM/yyyy", "yyyy-MM-dd", "MM/dd/yyyy",
            "d-M-yyyy", "d/M/yyyy", "M/d/yy", "d/M/yy", "dd/MM/yy", "MM/dd/yy"
        ]
        date_exprs = [to_date(col(column_name), f) for f in formats]
        
        # Bước 1: Parse ngày thô
        df = df.withColumn("tmp_date", coalesce(coalesce(*date_exprs), to_date(lit(fallback_date_str))))
        
        # Bước 2: Ép các năm ngáo (như 0025) cộng thêm 2000 năm để thành 2025
        df = df.withColumn(
            "transaction_date",
            when(
                year("tmp_date") < 100, 
                expr("make_date(year(tmp_date) + 2000, month(tmp_date), day(tmp_date))")
            ).otherwise(col("tmp_date"))
        ).drop("tmp_date")
        
        return df
    
    # ========================= HNF Section =========================
    
    def process_account_customer(self):
        self.log_info("Processing AccountCustomer CSV files...")
        try:
            pattern = f"{self.base_path}/hnf/AccountCustomer.*.csv"
            file_list = self.find_files_with_pattern(pattern)
            if not file_list: return
            
            cleared_partitions = set()
            count = 0
            
            for input_path in file_list:
                self.log_info(f"  Reading: {input_path}")
                filename = Path(input_path).name
                date_match = re.search(r'(\d{8})', filename)
                if not date_match: continue
                
                date_str = date_match.group(1)
                partition_date = f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"
                
                df = self.spark.read.option("header", "false").option("sep", "|").csv(input_path)
                schema_header = [
                    "ACCOUNT", "ACCOUNT_ID", "PRODCAT", "CID", "ACC_OPENED_DATE", 
                    "DOB", "NAME", "SHORT_NAME", "OTHER_NAME", "AMOUNT", "COMPANY", "GENDER"
                ]
                if len(df.columns) == len(schema_header):
                    for old_col, new_col in zip(df.columns, schema_header):
                        df = df.withColumnRenamed(old_col, new_col)
                
                df = df.withColumn("partition_date", lit(partition_date))
                output_path = f"{self.output_path}/hnf/account_customer"
                
                target_partition_path = f"{output_path}/partition_date={partition_date}"
                if target_partition_path not in cleared_partitions:
                    self.delete_path_if_exists(target_partition_path)
                    cleared_partitions.add(target_partition_path)
                
                df.write.mode("append").partitionBy("partition_date").parquet(output_path)
                count += 1
                
            self.log_info(f"  ✓ Processed {count} AccountCustomer files.")
        except Exception as e: self.log_info(f"  ✗ Error: {str(e)}")
    
    def process_crb_sheet4(self):
        self.log_info("Processing CRB Excel files (Sheet4)...")
        try:
            pattern = f"{self.base_path}/hnf/*_CRB.xlsx"
            file_list = self.find_files_with_pattern(pattern)
            if not file_list: return
            
            cleared_partitions = set()
            count = 0
            
            for input_path in file_list:
                self.log_info(f"  Reading: {input_path}")
                filename = Path(input_path).name
                month_match = re.search(r'(\d{4})_(\d{2})', filename)
                if not month_match: continue
                
                partition_month = f"{month_match.group(1)}-{month_match.group(2)}"
                
                df = self.spark.read.format("com.crealytics.spark.excel") \
                    .option("dataAddress", "'Sheet4'!A1") \
                    .option("header", "false") \
                    .option("maxRowsInMemory", 10000) \
                    .load(input_path)
                
                df = df.filter(col("_c0").isNotNull() & (trim(col("_c0")) != "LINE"))
                
                rename_map = {
                    0: "LINE",
                    1: "CURRENCY",
                    2: "CUSTOMER",
                    3: "CURRENCY_AMT",
                    4: "LOCAL_CCY_AMT",
                    5: "INT_RATE",
                    6: "INT",
                    7: "VALUE_DATE",
                    8: "MAT_DATE",
                    9: "BRANCH",        
                    10: "PRODCAT",      
                    13: "BRANCH_NAME"   
                }
                
                old_cols = df.columns
                for idx, new_name in rename_map.items():
                    if idx < len(old_cols):
                        df = df.withColumnRenamed(old_cols[idx], new_name)
                
                safe_line_col = col("LINE").cast("double").cast("decimal(20,0)").cast("string")
                df = df.withColumn("BRANCH_ID", substring(safe_line_col, 1, 1))
                df = df.withColumn("PRODCAT", substring(safe_line_col, 2, 4))
                
                df = df.withColumn("partition_month", lit(partition_month))
                output_path = f"{self.output_path}/hnf/crb"
                
                target_partition_path = f"{output_path}/partition_month={partition_month}"
                if target_partition_path not in cleared_partitions:
                    self.delete_path_if_exists(target_partition_path)
                    cleared_partitions.add(target_partition_path)
                
                df.write.mode("append").partitionBy("partition_month").parquet(output_path)
                count += 1
                
            self.log_info(f"  ✓ Processed {count} CRB files.")
        except Exception as e: self.log_info(f"  ✗ Error: {str(e)}")
    
    # ========================= AGENT BANKING Section =========================
    
    def process_agency_banking(self):
        self.log_info("Processing Agency Banking transactions files...")
        try:
            pattern = f"{self.base_path}/agent_banking/*_TransactionsLogReport_Agency_Banking_EOM.xlsx"
            file_list = self.find_files_with_pattern(pattern)
            if not file_list: return
            
            cleared_partitions = set()
            count = 0
            
            for input_path in file_list:
                self.log_info(f"  Reading: {input_path}")
                filename = Path(input_path).name
                month_match = re.search(r'(\d{4})_(\d{2})', filename)
                if not month_match: continue
                
                year_str, month_str = month_match.group(1), month_match.group(2)
                partition_month = f"{year_str}-{month_str}"
                fallback_date = f"{partition_month}-01"
                
                df = self.spark.read.format("com.crealytics.spark.excel") \
                    .option("header", "true").option("maxRowsInMemory", 10000).load(input_path)
                
                if "Transaction Date" in df.columns:
                    df = self.parse_date_robustly(df, "Transaction Date", fallback_date)
                else:
                    df = df.withColumn("transaction_date", lit(fallback_date))
                
                df = df.withColumn("partition_month", lit(partition_month))
                df = df.withColumn("partition_date", col("transaction_date").cast(StringType()))
                
                output_path = f"{self.output_path}/agent_banking/transactions_log"
                target_partition_path = f"{output_path}/partition_month={partition_month}"
                if target_partition_path not in cleared_partitions:
                    self.delete_path_if_exists(target_partition_path)
                    cleared_partitions.add(target_partition_path)
                
                df.write.mode("append").partitionBy("partition_month", "partition_date").parquet(output_path)
                count += 1
                
            self.log_info(f"  ✓ Processed {count} Agency Banking files.")
        except Exception as e: self.log_info(f"  ✗ Error: {str(e)}")
    
    # ========================= DIGITAL SERVICE Section =========================
    
    def process_atm_local_transactions(self):
        self.log_info("Processing ATM Local transactions files...")
        try:
            pattern = f"{self.base_path}/digital_service/*_Transaction_ATM_Local_EOM.csv"
            file_list = self.find_files_with_pattern(pattern)
            if not file_list: return
            
            cleared_partitions = set()
            count = 0
            
            for input_path in file_list:
                self.log_info(f"  Reading: {input_path}")
                filename = Path(input_path).name
                month_match = re.search(r'(\d{4})_(\d{2})', filename)
                partition_month = f"{month_match.group(1)}-{month_match.group(2)}" if month_match else "unknown"
                fallback_date = f"{partition_month}-01" if month_match else "1970-01-01"
                
                df = self.spark.read.option("header", "true") \
                    .option("multiLine", "true").option("escape", '"').csv(input_path)
                    
                if "msgtime" in df.columns:
                    df = self.parse_date_robustly(df, "msgtime", fallback_date)
                else:
                    df = df.withColumn("transaction_date", lit(fallback_date))
                
                df = df.withColumn("partition_month", lit(partition_month))
                df = df.withColumn("partition_date", col("transaction_date").cast(StringType()))
                
                output_path = f"{self.output_path}/digital_service/atm_local_transactions"
                target_partition_path = f"{output_path}/partition_month={partition_month}"
                if target_partition_path not in cleared_partitions:
                    self.delete_path_if_exists(target_partition_path)
                    cleared_partitions.add(target_partition_path)
                
                df.write.mode("append").partitionBy("partition_month", "partition_date").parquet(output_path)
                count += 1
                
            self.log_info(f"  ✓ Processed {count} ATM Local files.")
        except Exception as e: self.log_info(f"  ✗ Error: {str(e)}")
    
    def process_internet_banking_transactions(self):
        self.log_info("Processing Internet Banking transactions files...")
        try:
            pattern = f"{self.base_path}/digital_service/*_Transaction_Internet_Banking_EOM.xlsx"
            file_list = self.find_files_with_pattern(pattern)
            if not file_list: return
            
            cleared_partitions = set()
            count = 0
            
            for input_path in file_list:
                self.log_info(f"  Reading: {input_path}")
                filename = Path(input_path).name
                month_match = re.search(r'(\d{4})_(\d{2})', filename)
                partition_month = f"{month_match.group(1)}-{month_match.group(2)}" if month_match else "unknown"
                fallback_date = f"{partition_month}-01" if month_match else "1970-01-01"
                
                df = self.spark.read.format("com.crealytics.spark.excel") \
                    .option("header", "true").option("maxRowsInMemory", 10000).load(input_path)
                
                if "transferDateTime" in df.columns:
                    df = self.parse_date_robustly(df, "transferDateTime", fallback_date)
                else:
                    df = df.withColumn("transaction_date", lit(fallback_date))
                
                df = df.withColumn("partition_month", lit(partition_month))
                df = df.withColumn("partition_date", col("transaction_date").cast(StringType()))
                
                output_path = f"{self.output_path}/digital_service/internet_banking_transactions"
                target_partition_path = f"{output_path}/partition_month={partition_month}"
                if target_partition_path not in cleared_partitions:
                    self.delete_path_if_exists(target_partition_path)
                    cleared_partitions.add(target_partition_path)
                
                df.write.mode("append").partitionBy("partition_month", "partition_date").parquet(output_path)
                count += 1
                
            self.log_info(f"  ✓ Processed {count} Internet Banking files.")
        except Exception as e: self.log_info(f"  ✗ Error: {str(e)}")
    
    def process_transfer_report_csv(self, report_type):
        self.log_info(f"Processing {report_type} Transfer Report files...")
        try:
            pattern = f"{self.base_path}/digital_service/*_TransferReportBnctl_{report_type}_EOM.csv"
            file_list = self.find_files_with_pattern(pattern)
            if not file_list: return
            
            cleared_partitions = set()
            count = 0
            
            for input_path in file_list:
                self.log_info(f"  Reading: {input_path}")
                filename = Path(input_path).name
                month_match = re.search(r'(\d{4})_(\d{2})', filename)
                partition_month = f"{month_match.group(1)}-{month_match.group(2)}" if month_match else "unknown"
                fallback_date = f"{partition_month}-01" if month_match else "1970-01-01"
                
                df = self.spark.read.option("header", "true") \
                    .option("multiLine", "true").option("escape", '"').csv(input_path)
                
                if "Transaction Date" in df.columns:
                    df = self.parse_date_robustly(df, "Transaction Date", fallback_date)
                else:
                    df = df.withColumn("transaction_date", lit(fallback_date))
                
                df = df.withColumn("partition_month", lit(partition_month))
                df = df.withColumn("partition_date", col("transaction_date").cast(StringType()))
                
                output_path = f"{self.output_path}/digital_service/transfer_report_{report_type.lower()}"
                target_partition_path = f"{output_path}/partition_month={partition_month}"
                if target_partition_path not in cleared_partitions:
                    self.delete_path_if_exists(target_partition_path)
                    cleared_partitions.add(target_partition_path)
                
                df.write.mode("append").partitionBy("partition_month", "partition_date").parquet(output_path)
                count += 1
                
            self.log_info(f"  ✓ Processed {count} {report_type} Transfer Report files.")
        except Exception as e: self.log_info(f"  ✗ Error: {str(e)}")
    
    # ========================= Main Processing =========================
    
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
    spark = SparkSession.builder.appName("OperationSourceProcessor").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    try:
        processor = OperationSourceProcessor(spark)
        processor.process_all()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()