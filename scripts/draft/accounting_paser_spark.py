"""
GL Parser - PySpark Distributed Version
Parse General Ledger text files từ MinIO/S3 sử dụng Spark distributed processing.

Kiến trúc phân tán:
  - Driver: list files → sc.parallelize(file_paths)
  - Executor: mỗi task nhận 1 file path, tự kết nối MinIO, đọc + parse + upload CSV/stats
  - Driver: collect metadata (không phải data) về để chọn file tốt nhất
  - Zero collect() trên data thực — chỉ metadata dict được trả về
"""

import re
import io
import os
import sys
import json
import uuid
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from collections import defaultdict

from pyspark.sql import SparkSession
from minio import Minio
from minio.commonconfig import CopySource

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# MINIO CLIENT HELPER
# ─────────────────────────────────────────────
def create_minio_client(endpoint: str, access_key: str, secret_key: str) -> Minio:
    clean = endpoint
    if clean.startswith('https://'):
        clean = clean[8:]
        secure = True
    elif clean.startswith('http://'):
        clean = clean[7:]
        secure = False
    else:
        secure = True
    if '/' in clean:
        clean = clean.split('/')[0]
    return Minio(endpoint=clean, access_key=access_key, secret_key=secret_key, secure=secure)


# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────
def create_spark_session(endpoint: str, access_key: str, secret_key: str) -> SparkSession:
    spark = SparkSession.builder.appName("GL_Parser_PySpark_Distributed").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────
# PARSER FUNCTIONS (stateless, dùng cho distributed processing)
# ─────────────────────────────────────────────
class DistributedGLParser:
    """
    Stateles GL Parser - không lưu state giữa các partition.
    Mỗi partition tự parse độc lập, sau đó aggregate kết quả.
    """
    
    # Compile patterns một lần
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
        if not value or value.strip() == "":
            return None
        try:
            return float(value.replace(',', ''))
        except:
            return None

    @classmethod
    def is_page_header(cls, line: str) -> bool:
        return any(p in line for p in cls.SKIP_PATTERNS)

    @classmethod
    def is_end_marker(cls, line: str) -> bool:
        return 'END OF REPORT' in line

    @classmethod
    def parse_line(cls, line: str, line_num: int, state: Dict) -> Optional[Dict]:
        """Parse một dòng, cập nhật state và trả về result nếu có."""
        if not line.strip():
            state['last_row_was_data'] = False
            return None
        
        if cls.is_page_header(line):
            state['last_row_was_data'] = False
            return None
        
        if cls.DESCRIPTION_ONLY_PATTERN.match(line):
            return None

        # Continuation
        if cls.CONTINUATION_PATTERN.match(line):
            cont = line.strip()
            if state['pending_account_code']:
                state['pending_account_code'] += cont
                state['continuation_lines'] += 1
            elif state['last_result'] and state['last_row_was_data']:
                state['last_result']['description'] += cont
                state['continuation_lines'] += 1
            return None

        # Account code + data same line
        m = cls.ACCOUNT_WITH_DATA_PATTERN.match(line)
        if m:
            result = {
                'line': '',
                'description': m.group(1).strip(),
                'opening_balance': cls.clean_number(m.group(2)),
                'debit_movements': cls.clean_number(m.group(3)),
                'credit_movements': cls.clean_number(m.group(4)),
                'closing_balance': cls.clean_number(m.group(5)),
            }
            state['data_rows'] += 1
            state['account_codes'] += 1
            state['last_row_was_data'] = True
            state['last_result'] = result
            return result

        # Account code only
        if cls.ACCOUNT_CODE_PATTERN.match(line):
            state['pending_account_code'] = line.strip()
            state['account_codes'] += 1
            state['last_row_was_data'] = False
            return None

        # Data row with 4 numbers
        m = cls.DATA_ROW_PATTERN.match(line)
        if m:
            line_code = m.group(1) if m.group(1) else ""
            description = m.group(2).strip()
            if state['pending_account_code']:
                description = f"{state['pending_account_code']} {description}"
                state['pending_account_code'] = None
            result = {
                'line': line_code,
                'description': description,
                'opening_balance': cls.clean_number(m.group(3)),
                'debit_movements': cls.clean_number(m.group(4)),
                'credit_movements': cls.clean_number(m.group(5)),
                'closing_balance': cls.clean_number(m.group(6)),
            }
            state['data_rows'] += 1
            state['last_row_was_data'] = True
            state['last_result'] = result
            return result

        # Header row (no numbers)
        m = cls.HEADER_PATTERN.match(line)
        if m:
            result = {
                'line': m.group(1),
                'description': m.group(2).strip(),
                'opening_balance': None,
                'debit_movements': None,
                'credit_movements': None,
                'closing_balance': None,
            }
            state['header_rows'] += 1
            state['last_row_was_data'] = False
            state['last_result'] = result
            return result

        if line.strip():
            state['skipped_rows'] += 1
        state['last_row_was_data'] = False
        return None


# ─────────────────────────────────────────────
# FILE TYPE & BRANCH DETECTION
# ─────────────────────────────────────────────
def detect_file_type(text: str) -> str:
    header = text[:1500].upper()
    # Phải có cả RE000010 lẫn BNCTL — giống logic original
    if 'RE000010' not in header or 'BNCTL' not in header:
        return 'UNKNOWN'
    if 'BALANCE DETAILS' in header and 'IN USD' in header:
        return 'CRB'
    if 'PROFIT AND LOSS' in header:
        ledger = 'PL'
    elif 'GENERAL LEDGER' in header:
        ledger = 'GL'
    else:
        return 'UNKNOWN'
    prefix = 'CRC' if 'LIVE SYSTEM' in header else 'CRF'
    return f"{prefix}_{ledger}"


def extract_branch_name(text: str) -> str:
    re_pat = re.compile(r'^RE\d+')
    for line_num, raw_line in enumerate(text.splitlines(), 1):
        line = raw_line.strip()
        if not line:
            continue
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
                identifier = re.sub(r'_+', '_', identifier).strip('_')
                return identifier
        if line_num > 30:
            break
    return 'UNKNOWN_BRANCH'


# ─────────────────────────────────────────────
# LIST FILES FROM S3
# ─────────────────────────────────────────────
def list_s3_files(spark: SparkSession, bucket: str, prefix: str) -> List[str]:
    sc = spark.sparkContext
    jvm = sc._jvm
    conf = sc._jsc.hadoopConfiguration()
    s3a_path = f"s3a://{bucket}/{prefix}"
    logger.info(f"  list_s3_files: scanning {s3a_path}")
    uri = jvm.java.net.URI.create(s3a_path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
    path_obj = jvm.org.apache.hadoop.fs.Path(s3a_path)
    files = []
    try:
        it = fs.listFiles(path_obj, True)
        while it.hasNext():
            files.append(str(it.next().getPath().toString()))
        logger.info(f"  list_s3_files: found {len(files)} file(s)")
    except Exception as e:
        logger.error(f"Error listing S3 files at {s3a_path}: {e}", exc_info=True)
    return files


# ─────────────────────────────────────────────
# PROCESS FILES WITH SPARK (DISTRIBUTED)
# ─────────────────────────────────────────────
RAW_OUTPUT_PREFIX = "accounting_parsed_output/"


def _process_one_file(args: Tuple) -> None:
    """
    Chạy trên Spark EXECUTOR — dùng với rdd.foreach(), không return gì về driver.

    Luồng trên mỗi executor:
      1. Tạo MinIO client riêng
      2. Đọc file từ MinIO
      3. Parse từng dòng (stateful, sequential)
      4. Ghi CSV output lên MinIO
      5. Ghi stats.txt lên MinIO
      6. Ghi metadata JSON lên MinIO (thay thế cho return — driver đọc lại sau)
    """
    import io
    import re
    import json
    import logging
    import pandas as pd
    from minio import Minio

    s3_path, cfg = args
    raw_bucket  = cfg['raw_bucket']
    meta_prefix = cfg['meta_prefix']
    original_filename = s3_path.split('/')[-1]
    log = logging.getLogger("gl_executor")

    def _mk_minio(endpoint, access_key, secret_key):
        clean = endpoint
        if clean.startswith('https://'):
            clean, secure = clean[8:], True
        elif clean.startswith('http://'):
            clean, secure = clean[7:], False
        else:
            secure = True
        if '/' in clean:
            clean = clean.split('/')[0]
        return Minio(endpoint=clean, access_key=access_key, secret_key=secret_key, secure=secure)

    def _write_meta(client, meta: dict):
        """Ghi metadata JSON lên MinIO để driver đọc lại — không return về Spark."""
        buf = json.dumps(meta).encode('utf-8')
        key = meta_prefix + original_filename + '.json'
        client.put_object(raw_bucket, key, io.BytesIO(buf), len(buf),
                          content_type='application/json')

    client = _mk_minio(cfg['endpoint'], cfg['access_key'], cfg['secret_key'])

    try:
        # ── Đọc file ────────────────────────────────────────────────────
        path_no_scheme = s3_path.replace('s3a://', '').replace('s3://', '')
        bucket_name, obj_key = path_no_scheme.split('/', 1)
        response = client.get_object(bucket_name, obj_key)
        try:
            raw_bytes = response.read()
        finally:
            response.close()
            response.release_conn()

        text = raw_bytes.decode('utf-8', errors='replace')
        del raw_bytes

        # ── Detect type & branch ─────────────────────────────────────────
        file_type = detect_file_type(text)
        if file_type in ('CRB', 'UNKNOWN'):
            log.warning(f"[{original_filename}] Skip: {file_type}")
            _write_meta(client, {
                'file': original_filename, 's3_path': s3_path, 'success': False,
                'file_type': file_type, 'branch_identifier': None,
                'csv_path': None, 'txt_path': None,
                'output_rows': 0, 'datetime_key': '', 'error': f'{file_type} not supported',
            })
            return

        branch = extract_branch_name(text)

        # ── Parse ────────────────────────────────────────────────────────
        parser = DistributedGLParser()
        state = {
            'pending_account_code': None, 'last_row_was_data': False,
            'last_result': None, 'data_rows': 0, 'header_rows': 0,
            'skipped_rows': 0, 'account_codes': 0, 'continuation_lines': 0,
            'total_lines': 0,
        }
        lines_list = text.splitlines(keepends=True)
        state['total_lines'] = len(lines_list)
        del text

        parse_results = []
        for line_num, line in enumerate(lines_list, 1):
            if parser.is_end_marker(line):
                state['total_lines'] = line_num
                break
            result = parser.parse_line(line, line_num, state)
            if result:
                parse_results.append(result)
        del lines_list

        df = pd.DataFrame(parse_results)
        del parse_results
        if not df.empty:
            df.insert(0, 'order_id', range(1, len(df) + 1))
        log.info(f"[{original_filename}] Parsed {len(df)} rows | branch={branch}")

        output_base = f"{file_type}_{branch}_{original_filename}"

        # ── Ghi CSV lên MinIO ────────────────────────────────────────────
        csv_obj = f"{RAW_OUTPUT_PREFIX}{output_base}.csv"
        csv_buf = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
        client.put_object(raw_bucket, csv_obj,
                          io.BytesIO(csv_buf), len(csv_buf), content_type='text/csv')
        del csv_buf

        # ── Ghi stats lên MinIO ──────────────────────────────────────────
        totals = {
            col: float(df[col].fillna(0).sum()) if col in df.columns else 0.0
            for col in ('opening_balance', 'debit_movements', 'credit_movements', 'closing_balance')
        }
        output_rows = len(df)
        del df

        stats_buf = "\n".join([
            "GENERAL LEDGER PARSING STATISTICS", "=" * 80, "",
            f"S3 Path:            {s3_path}",
            f"File Type:          {file_type}",
            f"Branch:             {branch}",
            f"Total Lines:        {state['total_lines']:,}",
            f"Data Rows:          {state['data_rows']:,}",
            f"Header Rows:        {state['header_rows']:,}",
            f"Account Codes:      {state['account_codes']:,}",
            f"Continuation Lines: {state['continuation_lines']:,}",
            f"Skipped Rows:       {state['skipped_rows']:,}",
            f"Output Rows:        {output_rows:,}", "",
            "TOTALS", "-" * 80,
            f"Opening Balance:   ${totals['opening_balance']:,.2f}",
            f"Debit Movements:   ${totals['debit_movements']:,.2f}",
            f"Credit Movements:  ${totals['credit_movements']:,.2f}",
            f"Closing Balance:   ${totals['closing_balance']:,.2f}",
        ]).encode('utf-8')
        txt_obj = f"{RAW_OUTPUT_PREFIX}{output_base}_stats.txt"
        client.put_object(raw_bucket, txt_obj,
                          io.BytesIO(stats_buf), len(stats_buf), content_type='text/plain')

        # ── Extract datetime_key ─────────────────────────────────────────
        m_new = re.match(
            r'(?:CRF|CRC)_(?:GL|PL)_RE\d+_.*?_(\d{8})_(\d{8})_(\d{6})_\d+', output_base)
        m_old = re.match(r'(?:CRF|CRC)_(?:GL|PL)_RE\d+_.*?_(\d+)', output_base)
        if m_new:
            datetime_key = f"{m_new.group(2)}_{m_new.group(3)}"
        elif m_old:
            datetime_key = m_old.group(1)
        else:
            datetime_key = output_base

        # ── Ghi metadata JSON — thay cho return ─────────────────────────
        _write_meta(client, {
            'file': original_filename, 's3_path': s3_path, 'success': True,
            'file_type': file_type, 'branch_identifier': branch,
            'csv_path': csv_obj, 'txt_path': txt_obj,
            'output_rows': output_rows, 'datetime_key': datetime_key, 'error': None,
        })

    except Exception as e:
        import traceback
        log.error(f"[{original_filename}] ERROR: {e}\n{traceback.format_exc()}")
        try:
            _write_meta(client, {
                'file': original_filename, 's3_path': s3_path, 'success': False,
                'file_type': None, 'branch_identifier': None,
                'csv_path': None, 'txt_path': None,
                'output_rows': 0, 'datetime_key': '', 'error': str(e),
            })
        except Exception:
            pass


def process_files_distributed(
    spark: SparkSession,
    raw_bucket: str,
    s3_files: List[str],
    cob_date: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
) -> List[Dict]:
    """
    Hoàn toàn không dùng collect() trên data:
      1. foreach() — mỗi executor xử lý 1 file, ghi CSV + metadata JSON lên MinIO
      2. Driver đọc lại metadata JSON từ MinIO (vài trăm bytes/file)
      3. Xóa metadata JSON tạm sau khi đọc xong
    """
    sc = spark.sparkContext

    # Prefix riêng cho mỗi lần chạy để tránh đọc nhầm metadata cũ
    run_id = uuid.uuid4().hex[:8]
    meta_prefix = f"{RAW_OUTPUT_PREFIX}_meta/{cob_date}/{run_id}/"

    cfg = {
        'endpoint': endpoint,
        'access_key': access_key,
        'secret_key': secret_key,
        'raw_bucket': raw_bucket,
        'meta_prefix': meta_prefix,
    }

    file_args = [(f, cfg) for f in s3_files]
    num_partitions = len(s3_files)

    logger.info(f"Submitting {num_partitions} files to Spark executors (no collect)...")
    logger.info(f"Metadata prefix: {meta_prefix}")

    # foreach() thay cho map().collect() — không có data nào về driver
    sc.parallelize(file_args, numSlices=num_partitions).foreach(_process_one_file)

    # Driver đọc metadata JSON từ MinIO (chỉ vài trăm bytes/file)
    driver_client = create_minio_client(endpoint, access_key, secret_key)
    results: List[Dict] = []
    for obj in driver_client.list_objects(raw_bucket, prefix=meta_prefix, recursive=True):
        resp = driver_client.get_object(raw_bucket, obj.object_name)
        try:
            results.append(json.loads(resp.read().decode('utf-8')))
        finally:
            resp.close()
            resp.release_conn()
        # Xóa metadata tạm ngay sau khi đọc
        driver_client.remove_object(raw_bucket, obj.object_name)

    for r in results:
        status = "✓" if r['success'] else "✗"
        logger.info(f"  {status} {r['file']}  rows={r['output_rows']}  err={r.get('error')}")

    return results


# ─────────────────────────────────────────────
# FIND LATEST SUMMARY FILES (giữ nguyên logic)
# ─────────────────────────────────────────────
def find_latest_summary_files(all_results: List[Dict], cob_date: str) -> Dict:
    TARGET_ROWS = {'GL': 654, 'PL': 242}
    all_files = {'GL': {}, 'PL': {}}
    detailed_files = {'GL': {}, 'PL': {}}

    for res in all_results:
        if not res['success']:
            continue
        ft = res.get('file_type', '')
        if not ft or '_' not in ft:
            continue
        ledger_type = ft.split('_')[-1]
        if ledger_type not in ('GL', 'PL'):
            continue

        branch = res['branch_identifier']
        datetime_key = res['datetime_key']
        row_count = res['output_rows']

        file_info = {
            'file': res['csv_path'],
            'txt_file': res['txt_path'],
            'datetime_key': datetime_key,
            'rows': row_count,
            'file_type': ft,
        }

        all_files[ledger_type].setdefault(branch, []).append(file_info)

        if row_count > TARGET_ROWS[ledger_type]:
            if branch not in detailed_files[ledger_type]:
                detailed_files[ledger_type][branch] = file_info
                logger.info(f"Phase 1: First detailed {ledger_type} for {branch} ({row_count} rows)")
            elif datetime_key > detailed_files[ledger_type][branch]['datetime_key']:
                logger.info(f"Phase 1: Updated {ledger_type} for {branch} ({row_count} rows)")
                detailed_files[ledger_type][branch] = file_info

    logger.info("--- Phase 2: Fallback ---")
    for ledger_type in ('GL', 'PL'):
        for branch, files_list in all_files[ledger_type].items():
            if branch not in detailed_files[ledger_type]:
                latest = max(files_list, key=lambda x: x['datetime_key'])
                detailed_files[ledger_type][branch] = latest
                logger.info(
                    f"Phase 2 Fallback: {ledger_type} | {branch} | "
                    f"{Path(latest['file']).name} ({latest['rows']} rows)"
                )

    csv_files = []
    txt_files = []
    logger.info("--- Final Selection ---")
    for ledger_type in ('GL', 'PL'):
        target = TARGET_ROWS[ledger_type]
        for branch, info in detailed_files[ledger_type].items():
            phase = "Phase 1 DETAILED" if info['rows'] > target else "Phase 2 FALLBACK"
            logger.info(
                f"  ✓ {ledger_type} | {branch} | "
                f"{Path(info['file']).name} ({info['rows']} rows) [{phase}]"
            )
            csv_files.append(info['file'])
            if info['txt_file']:
                txt_files.append(info['txt_file'])

    return {'csv_files': csv_files, 'txt_files': txt_files, 'date_folder': cob_date}


# ─────────────────────────────────────────────
# UPLOAD TO BRONZE MINIO
# ─────────────────────────────────────────────
def upload_to_bronze_minio(
    latest_files: Dict,
    endpoint: str,
    access_key: str,
    secret_key: str,
    raw_bucket: str,
) -> bool:
    try:
        client = create_minio_client(endpoint, access_key, secret_key)
        bronze_bucket = "bronze"
        date_folder = latest_files['date_folder']
        base_prefix = "accounting_parsed_spark"

        logger.info(f"Copying {len(latest_files['csv_files'])} CSV → s3://{bronze_bucket}/{base_prefix}/data/...")
        for csv_obj in latest_files['csv_files']:
            filename = Path(csv_obj).name
            m = re.match(
                r'(CRF|CRC)_(GL|PL)_(RE\d+_.*?)_(\d{8})_(\d{8})_(\d{6})_(\d+)\.csv',
                filename
            )
            if m:
                branch_name = m.group(3)
                file_type_dir = f"{m.group(1)}_{m.group(2)}"
                dest_obj = f"{base_prefix}/data/{date_folder}/{branch_name}/{file_type_dir}/{filename}"
            else:
                dest_obj = f"{base_prefix}/data/{date_folder}/UNKNOWN_BRANCH/UNKNOWN_TYPE/{filename}"
                logger.warning(f"  ⚠ Filename không match format: {filename}")
            client.copy_object(bronze_bucket, dest_obj, CopySource(raw_bucket, csv_obj))
            logger.info(f"  ✓ s3://{bronze_bucket}/{dest_obj}")

        logger.info(f"Copying {len(latest_files['txt_files'])} TXT → s3://{bronze_bucket}/{base_prefix}/summary/...")
        for txt_obj in latest_files['txt_files']:
            filename = Path(txt_obj).name
            dest_obj = f"{base_prefix}/summary/{date_folder}/{filename}"
            client.copy_object(bronze_bucket, dest_obj, CopySource(raw_bucket, txt_obj))
            logger.info(f"  ✓ s3://{bronze_bucket}/{dest_obj}")

        logger.info("=" * 80)
        logger.info("BRONZE MINIO UPLOAD COMPLETED")
        return True

    except Exception as e:
        logger.error(f"Failed to upload to bronze MinIO: {e}")
        return False


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def _extract_date_from_path(s3_path: str) -> str:
    parts = s3_path.replace('s3a://', '').split('/')
    for part in parts:
        if re.match(r'^\d{4}-\d{2}-\d{2}$', part):
            return part
    return 'unknown'


def main():
    parser = argparse.ArgumentParser(
        description='Parse General Ledger text files via PySpark (Distributed)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--endpoint', default=os.getenv('MINIO_ENDPOINT'))
    parser.add_argument('--bucket', default=os.getenv('MINIO_BUCKET', 'raw'))
    parser.add_argument('--access-key', default=os.getenv('MINIO_ACCESS_KEY'))
    parser.add_argument('--secret-key', default=os.getenv('MINIO_SECRET_KEY'))
    parser.add_argument('--prefix', default=os.getenv('MINIO_PREFIX', 'accounting_full/'))
    parser.add_argument('--date', default=None)
    parser.add_argument('--upload-bronze', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    prefix = args.prefix
    if args.date:
        prefix = prefix.rstrip('/') + f'/{args.date}/'

    logger.info("=" * 80)
    logger.info("GL PARSER - PYSPARK DISTRIBUTED VERSION")
    logger.info("=" * 80)
    logger.info(f"Endpoint: {args.endpoint}")
    logger.info(f"Bucket:   {args.bucket}")
    logger.info(f"Prefix:   {prefix}")
    logger.info("=" * 80)

    spark = create_spark_session(args.endpoint, args.access_key, args.secret_key)
    logger.info("Spark session OK")

    logger.info(f"\nListing s3a://{args.bucket}/{prefix} ...")
    s3_files = list_s3_files(spark, args.bucket, prefix)

    if not s3_files:
        logger.error("No files found.")
        spark.stop()
        sys.exit(1)

    from collections import defaultdict
    files_by_date = defaultdict(list)
    for f in s3_files:
        date = _extract_date_from_path(f)
        files_by_date[date].append(f)

    dates = sorted(files_by_date.keys())
    logger.info(f"Found {len(s3_files)} file(s) across {len(dates)} date(s): {dates}")

    total_success = total_skip = total_fail = 0

    for date_idx, cob_date in enumerate(dates, 1):
        date_files = files_by_date[cob_date]
        logger.info(f"\n{'='*80}")
        logger.info(f"DATE [{date_idx}/{len(dates)}]: {cob_date}  ({len(date_files)} files)")
        logger.info(f"{'='*80}")

        all_results = process_files_distributed(
            spark, args.bucket, date_files, cob_date,
            args.endpoint, args.access_key, args.secret_key,
        )

        success_count = sum(1 for r in all_results if r['success'])
        skip_count = sum(1 for r in all_results if r.get('error') in ('CRB not supported', 'UNKNOWN not supported'))
        fail_count = len(all_results) - success_count - skip_count

        total_success += success_count
        total_skip += skip_count
        total_fail += fail_count

        if success_count > 0:
            logger.info(f"\n--- Phase 1/2 selection for {cob_date} ---")
            latest_files = find_latest_summary_files(all_results, cob_date)
            logger.info(f"Selected: {len(latest_files['csv_files'])} CSV, {len(latest_files['txt_files'])} TXT")

            if latest_files['csv_files'] and args.upload_bronze:
                ok = upload_to_bronze_minio(
                    latest_files, args.endpoint, args.access_key, args.secret_key, args.bucket
                )
                logger.info("✓ Bronze MinIO upload completed" if ok else "✗ Bronze MinIO upload failed")

    logger.info("\n" + "=" * 80)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Dates:    {len(dates)} ({', '.join(dates)})")
    logger.info(f"Total:    {len(s3_files)}")
    logger.info(f"Success:  {total_success}")
    logger.info(f"Skipped:  {total_skip}")
    logger.info(f"Failed:   {total_fail}")
    logger.info("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()
