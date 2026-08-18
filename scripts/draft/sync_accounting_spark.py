"""
Sync Accounting - PySpark Version
SFTP (Windows Server) → MinIO/S3, song song hoá bằng Spark.

Flow:
  Driver : kết nối SFTP, liệt kê toàn bộ file cần xử lý trong khoảng ngày
  Executors: mỗi executor nhận 1 batch file, tự kết nối SFTP + upload MinIO độc lập
"""

import re
import io
import os
import sys
import logging
import argparse
from datetime import datetime, timedelta

import paramiko
import boto3
import urllib3
from botocore.config import Config
from pyspark.sql import SparkSession

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG  (Lấy từ Environment Variables)
# ─────────────────────────────────────────────
SFTP_HOST      = os.environ.get('SFTP_HOST')
SFTP_PORT      = int(os.environ.get('SFTP_PORT', 22))
SFTP_USER      = os.environ.get('SFTP_USER')
SFTP_PASS      = os.environ.get('SFTP_PASS')
REMOTE_DIR     = os.environ.get('REMOTE_DIR')

MINIO_ENDPOINT   = os.environ.get('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
BUCKET_NAME      = os.environ.get('BUCKET_NAME')

# Default: ngày hôm qua nếu không set
DEFAULT_DATE = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
START_DATE_STR = os.environ.get('START_DATE') or DEFAULT_DATE
END_DATE_STR   = os.environ.get('END_DATE') or DEFAULT_DATE

CORE_KEYWORDS       = ["LINE", "DESCRIPTION", "OPENING", "DEBIT", "CREDIT", "CLOSING", "BALANCE"]
MULTIPART_THRESHOLD = 10 * 1024 * 1024   # 10 MB
CHUNK_SIZE          = 8 * 1024 * 1024    # 8 MB


# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    spark = SparkSession.builder.appName("Sync_Accounting_PySpark").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────
# UPLOAD TO MINIO  (chạy trên executor)
# ─────────────────────────────────────────────
def upload_to_minio(s3_client, bucket: str, key: str, file_obj, file_size: int):
    if file_size < MULTIPART_THRESHOLD:
        data = file_obj.read()
        s3_client.put_object(Bucket=bucket, Key=key, Body=data, ContentLength=len(data))
    else:
        mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
        parts = []
        part_num = 1
        try:
            while True:
                chunk = file_obj.read(CHUNK_SIZE)
                if not chunk:
                    break
                resp = s3_client.upload_part(
                    Bucket=bucket, Key=key,
                    PartNumber=part_num, UploadId=mpu['UploadId'], Body=chunk
                )
                parts.append({'PartNumber': part_num, 'ETag': resp['ETag']})
                part_num += 1
            s3_client.complete_multipart_upload(
                Bucket=bucket, Key=key,
                UploadId=mpu['UploadId'],
                MultipartUpload={'Parts': parts}
            )
        except Exception as e:
            s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=mpu['UploadId'])
            raise e


# ─────────────────────────────────────────────
# PROCESS ONE FILE  (chạy trên executor)
# ─────────────────────────────────────────────
def process_file(file_info: dict, sftp_creds: dict, minio_creds: dict) -> dict:
    """
    Mỗi executor tự kết nối SFTP + MinIO, xử lý 1 file độc lập.
    """
    original_name = file_info['name']
    remote_path   = file_info['remote_path']
    timestamp     = file_info['timestamp']

    result = {
        'file':     original_name,
        'success':  False,
        'skipped':  False,
        'minio_key': None,
        'error':    None,
    }

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(
            sftp_creds['host'], sftp_creds['port'],
            sftp_creds['user'], sftp_creds['pass'],
            timeout=120, auth_timeout=120
        )
        sftp = ssh.open_sftp()

        file_stat = sftp.stat(remote_path)
        file_size = file_stat.st_size

        with sftp.open(remote_path, 'rb') as f:
            raw_head = f.read(20000)
            text = raw_head.decode('latin-1', 'ignore').upper()

            date_match    = re.search(r"AS AT CLOSE OF\s+(\d{1,2})\s+([A-Z]{3})\s+(\d{4})", text)
            matched_count = sum(1 for w in CORE_KEYWORDS if w in text)

            if date_match and matched_count >= 5:
                day, month_str, year = date_match.groups()
                biz_dt            = datetime.strptime(f"{day} {month_str} {year}", "%d %b %Y")
                formatted_biz_date = biz_dt.strftime("%Y%m%d")
                folder_biz_date   = biz_dt.strftime("%Y-%m-%d")

                new_name   = f"{formatted_biz_date}_{timestamp}_{original_name}"
                minio_key  = f"accounting_full/{folder_biz_date}/{new_name}"

                s3_config = Config(
                    connect_timeout=300,
                    read_timeout=300,
                    retries={'max_attempts': 3, 'mode': 'adaptive'}
                )
                s3_client = boto3.client(
                    's3',
                    endpoint_url=minio_creds['endpoint'],
                    aws_access_key_id=minio_creds['access_key'],
                    aws_secret_access_key=minio_creds['secret_key'],
                    verify=False,
                    config=s3_config
                )

                f.seek(0)
                upload_to_minio(s3_client, minio_creds['bucket'], minio_key, f, file_size)

                result.update({
                    'success':    True,
                    'minio_key':  minio_key,
                    'biz_folder': folder_biz_date,
                })
                logger.info(f"[OK] {original_name} → {minio_key}")
            else:
                result['skipped'] = True
                logger.info(f"[SKIP] {original_name}: không thỏa điều kiện nội dung")

        sftp.close()

    except Exception as e:
        result['error'] = str(e)
        logger.error(f"[ERR] {original_name}: {e}")
    finally:
        ssh.close()

    return result


# ─────────────────────────────────────────────
# LIST FILES FROM SFTP  (chạy trên driver)
# ─────────────────────────────────────────────
def list_files_for_date(sftp_client, ssh_client, remote_dir: str, target_date: str) -> list:
    ps_command = (
        f'powershell.exe -Command "'
        f'Get-ChildItem -Path \'{remote_dir}\' -File | '
        f'Where-Object {{ $_.LastWriteTime.ToString(\'yyyy-MM-dd\') -eq \'{target_date}\' }} | '
        f'ForEach-Object {{ $_.Name + \'|\' + $_.LastWriteTime.ToString(\'yyyyMMdd_HHmmss\') }}"'
    )
    stdin, stdout, stderr = ssh_client.exec_command(ps_command)
    results = stdout.read().decode('utf-8').splitlines()

    files = []
    for line in results:
        if '|' not in line:
            continue
        name, timestamp = line.strip().split('|')
        files.append({
            'name':        name,
            'timestamp':   timestamp,
            'remote_path': f"{remote_dir}/{name}",
        })
    return files


# ─────────────────────────────────────────────
# CREATE _SUCCESS MARKERS  (chạy trên driver)
# ─────────────────────────────────────────────
def create_success_markers(uploaded_folders: set, minio_creds: dict):
    s3_config = Config(retries={'max_attempts': 3, 'mode': 'adaptive'})
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_creds['endpoint'],
        aws_access_key_id=minio_creds['access_key'],
        aws_secret_access_key=minio_creds['secret_key'],
        verify=False,
        config=s3_config
    )
    for folder in uploaded_folders:
        key = f"accounting_full/{folder}/_SUCCESS"
        try:
            s3_client.put_object(Bucket=minio_creds['bucket'], Key=key, Body=b'')
            logger.info(f"[SUCCESS] Tạo marker: {key}")
        except Exception as e:
            logger.error(f"[ERR] Không tạo được _SUCCESS tại {folder}: {e}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description='Sync accounting files SFTP → MinIO via PySpark',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Chạy với env vars:
  python sync_accounting_spark.py

  # Override ngày:
  python sync_accounting_spark.py --start-date 2025-01-22 --end-date 2025-01-25
        """
    )

    # Date range (override env vars)
    parser.add_argument('--start-date', default=START_DATE_STR,
                        help='YYYY-MM-DD')
    parser.add_argument('--end-date',   default=END_DATE_STR,
                        help='YYYY-MM-DD')

    parser.add_argument('-v', '--verbose', action='store_true')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    sftp_creds = {
        'host': SFTP_HOST,
        'port': SFTP_PORT,
        'user': SFTP_USER,
        'pass': SFTP_PASS,
    }
    minio_creds = {
        'endpoint':   MINIO_ENDPOINT,
        'access_key': MINIO_ACCESS_KEY,
        'secret_key': MINIO_SECRET_KEY,
        'bucket':     BUCKET_NAME,
    }

    logger.info("=" * 80)
    logger.info("SYNC ACCOUNTING - PYSPARK VERSION")
    logger.info("=" * 80)
    logger.info(f"SFTP Host   : {SFTP_HOST}:{SFTP_PORT}")
    logger.info(f"Remote Dir  : {REMOTE_DIR}")
    logger.info(f"MinIO       : {MINIO_ENDPOINT}  bucket={BUCKET_NAME}")
    logger.info(f"Date range  : {args.start_date} → {args.end_date}")
    logger.info("=" * 80)

    spark = create_spark_session()
    sc    = spark.sparkContext

    # Broadcast credentials sang executors
    bc_sftp  = sc.broadcast(sftp_creds)
    bc_minio = sc.broadcast(minio_creds)

    # ── Driver: kết nối SFTP, liệt kê file ───────────────────
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        logger.info(f"[*] Kết nối SFTP {SFTP_HOST}...")
        ssh.connect(
            SFTP_HOST, SFTP_PORT,
            SFTP_USER, SFTP_PASS,
            timeout=120, auth_timeout=120
        )
        sftp = ssh.open_sftp()

        start_dt = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_dt   = datetime.strptime(args.end_date,   '%Y-%m-%d')

        all_files = []
        current_dt = start_dt
        while current_dt <= end_dt:
            target_date = current_dt.strftime('%Y-%m-%d')
            logger.info(f"--- Liệt kê file ngày: {target_date} ---")
            files = list_files_for_date(sftp, ssh, REMOTE_DIR, target_date)
            logger.info(f"    Tìm thấy {len(files)} file")
            all_files.extend(files)
            current_dt += timedelta(days=1)

        sftp.close()
        ssh.close()

    except Exception as e:
        logger.critical(f"[X] Lỗi kết nối SFTP: {e}")
        spark.stop()
        sys.exit(1)

    if not all_files:
        logger.warning("Không có file nào để xử lý.")
        spark.stop()
        return

    logger.info(f"\nTổng số file cần xử lý: {len(all_files)}")

    # ── Spark: xử lý song song ────────────────────────────────
    files_rdd = sc.parallelize(all_files, numSlices=min(len(all_files), 8))

    def process_partition(files):
        sftp_c  = bc_sftp.value
        minio_c = bc_minio.value
        for f in files:
            yield process_file(f, sftp_c, minio_c)

    results = files_rdd.mapPartitions(process_partition).collect()

    # ── Driver: tổng hợp kết quả + tạo _SUCCESS ───────────────
    uploaded_folders = set()
    success = skip = fail = 0

    for r in results:
        if r['success']:
            success += 1
            uploaded_folders.add(r.get('biz_folder', ''))
        elif r['skipped']:
            skip += 1
        else:
            fail += 1

    uploaded_folders.discard('')

    if uploaded_folders:
        logger.info(f"\nTạo _SUCCESS marker cho {len(uploaded_folders)} folder(s)...")
        create_success_markers(uploaded_folders, minio_creds)

    logger.info("\n" + "=" * 80)
    logger.info("TỔNG KẾT")
    logger.info("=" * 80)
    logger.info(f"Tổng file    : {len(all_files)}")
    logger.info(f"Thành công   : {success}")
    logger.info(f"Bỏ qua       : {skip}")
    logger.info(f"Lỗi          : {fail}")
    logger.info(f"BizDate folders: {sorted(uploaded_folders)}")
    logger.info("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()
