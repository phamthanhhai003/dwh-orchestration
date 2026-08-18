"""
GL Parser - PySpark Lakehouse Version (Iceberg)
Kiến trúc: S3 (Raw Text) -> Spark RDD (Parse in-memory) -> DataFrame -> Iceberg (Overwrite Partitions)
"""

import re
import sys
import logging
import argparse
from typing import Dict, Optional

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit, to_date

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 1. PARSER CLASSES (LOGIC XỬ LÝ CHUỖI T24)
# ─────────────────────────────────────────────
class DistributedGLParser:
    ACCOUNT_CODE_PATTERN = re.compile(r'^\s+(AC\.|MD\.)')
    CONTINUATION_PATTERN = re.compile(r'^\s+[.\dTLA-Z]+[\w.]*$')
    DATA_ROW_PATTERN = re.compile(
        r'^\s*(\d{4})?\s+(.+?)\s+'
        r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s+'
        r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s+'
        r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s+'
        r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s*$'
    )
    ACCOUNT_WITH_DATA_PATTERN = re.compile(
        r'^\s+((?:AC|MD)\.[^\s]+)\s+'
        r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s+'
        r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s+'
        r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s+'
        r'(-?\d{1,3}(?:,\d{3})*\.\d{2})\s*$'
    )
    HEADER_PATTERN = re.compile(r'^\s*(\d{4})\s+([A-Za-z\s].+?)\s*$')
    DESCRIPTION_ONLY_PATTERN = re.compile(
        r'^\s+(?:[A-Z0-9]+\s+)?\([^)]*\)?\s*$|^\s+[a-zA-Z][a-zA-Z\s\-,&./]*\)\s*$'
    )
    SKIP_PATTERNS = [
        'RE000010', 'AREA NAME', 'PAGE NO',
        'LINE  DESCRIPTION', '----  -----------',
        'PRINTED AT', '( IN FULL AMOUNT )',
        'OPENING BALANCE', 'DEBIT MOVEMENTS'
    ]

    @staticmethod
    def clean_number(value: str) -> Optional[float]:
        if not value or value.strip() == "": return None
        try: return float(value.replace(',', ''))
        except: return None

    @classmethod
    def is_page_header(cls, line: str) -> bool:
        return any(p in line for p in cls.SKIP_PATTERNS)

    @classmethod
    def is_end_marker(cls, line: str) -> bool:
        return 'END OF REPORT' in line

    @classmethod
    def parse_line(cls, line: str, line_num: int, state: Dict) -> Optional[Dict]:
        if not line.strip():
            state['last_row_was_data'] = False
            return None
        
        if cls.is_page_header(line):
            state['last_row_was_data'] = False
            return None
        
        if cls.DESCRIPTION_ONLY_PATTERN.match(line):
            return None

        if cls.CONTINUATION_PATTERN.match(line):
            cont = line.strip()
            if state['pending_account_code']:
                state['pending_account_code'] += cont
            elif state['last_result'] and state['last_row_was_data']:
                state['last_result']['description'] += cont
            return None

        m = cls.ACCOUNT_WITH_DATA_PATTERN.match(line)
        if m:
            result = {
                'line_code': '',
                'description': m.group(1).strip(),
                'opening_balance': cls.clean_number(m.group(2)),
                'debit_movements': cls.clean_number(m.group(3)),
                'credit_movements': cls.clean_number(m.group(4)),
                'closing_balance': cls.clean_number(m.group(5)),
            }
            state['last_row_was_data'] = True
            state['last_result'] = result
            return result

        if cls.ACCOUNT_CODE_PATTERN.match(line):
            state['pending_account_code'] = line.strip()
            state['last_row_was_data'] = False
            return None

        m = cls.DATA_ROW_PATTERN.match(line)
        if m:
            line_code = m.group(1) if m.group(1) else ""
            description = m.group(2).strip()
            if state['pending_account_code']:
                description = f"{state['pending_account_code']} {description}"
                state['pending_account_code'] = None
            result = {
                'line_code': line_code,
                'description': description,
                'opening_balance': cls.clean_number(m.group(3)),
                'debit_movements': cls.clean_number(m.group(4)),
                'credit_movements': cls.clean_number(m.group(5)),
                'closing_balance': cls.clean_number(m.group(6)),
            }
            state['last_row_was_data'] = True
            state['last_result'] = result
            return result

        m = cls.HEADER_PATTERN.match(line)
        if m:
            result = {
                'line_code': m.group(1),
                'description': m.group(2).strip(),
                'opening_balance': None,
                'debit_movements': None,
                'credit_movements': None,
                'closing_balance': None,
            }
            state['last_row_was_data'] = False
            state['last_result'] = result
            return result

        state['last_row_was_data'] = False
        return None

def detect_file_type(text: str) -> str:
    header = text[:1500].upper()
    if 'RE000010' not in header or 'BNCTL' not in header: return 'UNKNOWN'
    if 'BALANCE DETAILS' in header and 'IN USD' in header: return 'CRB'
    if 'PROFIT AND LOSS' in header: ledger = 'PL'
    elif 'GENERAL LEDGER' in header: ledger = 'GL'
    else: return 'UNKNOWN'
    prefix = 'CRC' if 'LIVE SYSTEM' in header else 'CRF'
    return f"{prefix}_{ledger}"

def extract_branch_name(text: str) -> str:
    re_pat = re.compile(r'^RE\d+')
    for line_num, raw_line in enumerate(text.splitlines(), 1):
        line = raw_line.strip()
        if not line: continue
        if re_pat.match(line):
            parts = line.split()
            if len(parts) >= 2:
                try:
                    bnctl_idx = next(i for i, p in enumerate(parts) if 'BNCTL' in p)
                    branch_parts = parts[0:bnctl_idx]
                except StopIteration:
                    branch_parts = parts[0:4]
                identifier = '_'.join(branch_parts)
                identifier = re.sub(r'[^A-Za-z0-9_]', '_', identifier).upper()
                return re.sub(r'_+', '_', identifier).strip('_')
        if line_num > 30: break
    return 'UNKNOWN_BRANCH'

# ─────────────────────────────────────────────
# 2. HÀM MAPPER (CHẠY TRÊN EXECUTOR)
# ─────────────────────────────────────────────
def process_file_content(record) -> list:
    s3_path, content = record
    original_filename = s3_path.split('/')[-1]
    
    file_type = detect_file_type(content)
    if file_type in ('CRB', 'UNKNOWN'):
        return [] 
        
    branch = extract_branch_name(content)
    parser = DistributedGLParser()
    state = {'pending_account_code': None, 'last_row_was_data': False, 'last_result': None}
    
    parsed_rows = []
    for line_num, line in enumerate(content.splitlines(keepends=True), 1):
        if parser.is_end_marker(line):
            break
            
        result = parser.parse_line(line, line_num, state)
        if result:
            result['source_file'] = original_filename
            result['branch'] = branch
            result['file_type'] = file_type
            # line_seq = số dòng trong file (monotonic). Khoá ORDER BY DUY NHẤT để xác định
            # quan hệ cha-con của trial_balance_detail: mẹ của 1 detail row (line_code rỗng)
            # là header line NGAY BÊN DƯỚI nó → cần thứ tự dòng ổn định (Iceberg read KHÔNG
            # đảm bảo thứ tự). 1 file = 1 branch + 1 file_type nên line_seq đủ unique trong partition.
            result['line_seq'] = line_num
            parsed_rows.append(Row(**result))
            
    return parsed_rows

# ─────────────────────────────────────────────
# 3. LUỒNG DRIVER CHÍNH (ICEBERG PIPELINE)
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', default='raw-zone')
    parser.add_argument('--prefix', default='accounting/')
    parser.add_argument('--date', required=True)
    parser.add_argument('--table', default='hive.bronze.accounting_parser')
    # Tên cột partition. Default 'process_date' giữ tương thích DAG accounting cũ;
    # luồng sftp_hold truyền 'business_date' để khớp convention pull/sftp + reconcile_seal.
    parser.add_argument('--partition-col', default='process_date')
    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info(f"KHỞI ĐỘNG LUỒNG GL PARSER -> ICEBERG | DATE: {args.date}")
    logger.info("=" * 80)

    spark = SparkSession.builder.appName(f"GL-Parser-Iceberg-{args.date}").getOrCreate()
    sc = spark.sparkContext
    
    input_path = f"s3a://{args.bucket}/{args.prefix}{args.date}/[^_]*"
    logger.info(f"Đang phân phối đọc file tại: {input_path}")

    # Load file phân tán & Parse
    rdd_files = sc.wholeTextFiles(input_path)
    rdd_parsed = rdd_files.flatMap(process_file_content)
    
    if rdd_parsed.isEmpty():
        logger.error("Không có dữ liệu hợp lệ được parse. Dừng Job!")
        spark.stop()
        sys.exit(0)

    # DataFrame creation & Date Injection
    df_parsed = spark.createDataFrame(rdd_parsed)
    df_ready_to_write = df_parsed.withColumn(args.partition_col, to_date(lit(args.date)))
    
    row_count = df_ready_to_write.count()
    logger.info(f"✅ Parser hoàn tất. Chuẩn bị ghi {row_count} dòng xuống Iceberg.")

    # THỰC THI GHI ICEBERG (Logic Tự Tạo Bảng)
    table_name = args.table
    
    if not spark.catalog.tableExists(table_name):
        logger.info(f"✨ Bảng {table_name} chưa tồn tại. Đang tạo mới và thiết lập Partition...")
        df_ready_to_write.writeTo(table_name) \
            .tableProperty("format-version", "2") \
            .partitionedBy(args.partition_col) \
            .create()
    else:
        logger.info(f"🔄 Bảng {table_name} đã tồn tại. Đang OVERWRITE PARTITION cho ngày: {args.date}...")
        # Schema evolution: bảng cũ chưa có line_seq → ALTER ADD COLUMN trước (idempotent).
        existing_cols = [f.name for f in spark.table(table_name).schema.fields]
        if 'line_seq' not in existing_cols:
            logger.info(f"➕ Bảng thiếu cột line_seq → ALTER TABLE {table_name} ADD COLUMN line_seq INT")
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMN line_seq INT")
        # Align thứ tự cột df theo schema bảng (Iceberg writeTo check-ordering) trước khi ghi.
        table_cols = [f.name for f in spark.table(table_name).schema.fields]
        df_ready_to_write.select(*table_cols) \
            .writeTo(table_name) \
            .overwritePartitions()
        
    logger.info("🎉 HOÀN TẤT PIPELINE THÀNH CÔNG!")
    spark.stop()

if __name__ == "__main__":
    main()