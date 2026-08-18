"""
PySpark Script: Sync T24 files từ Windows SFTP → MinIO (Raw bucket)
====================================================================
Pattern: Driver list files → Spark Executor upload song song
- Driver : kết nối SFTP, liệt kê file theo date range, detect business date
- Executor: mỗi executor nhận 1 file, tự mở SFTP + upload MinIO độc lập
- Sau khi collect kết quả → Driver tạo _SUCCESS marker cho từng biz folder
"""

import os
import re
import paramiko
import boto3
import urllib3
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from datetime import datetime, timedelta
from botocore.config import Config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True
)

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG — đọc từ Environment Variables (giống script gốc)
# ══════════════════════════════════════════════════════════════════════════════
SFTP_HOST       = os.environ.get('SFTP_HOST')
SFTP_PORT       = int(os.environ.get('SFTP_PORT', 22))
SFTP_USER       = os.environ.get('SFTP_USER')
SFTP_PASS       = os.environ.get('SFTP_PASS')
REMOTE_DIR      = os.environ.get('REMOTE_DIR')
BUCKET_NAME     = os.environ.get('BUCKET_NAME')
MINIO_ENDPOINT  = os.environ.get('MINIO_ENDPOINT')
MINIO_ACCESS_KEY= os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY= os.environ.get('MINIO_SECRET_KEY')
START_DATE_STR  = os.environ.get('START_DATE')
END_DATE_STR    = os.environ.get('END_DATE')

# Giữ nguyên từ script gốc
CORE_KEYWORDS       = ["LINE", "DESCRIPTION", "OPENING", "DEBIT", "CREDIT", "CLOSING", "BALANCE"]
MULTIPART_THRESHOLD = 10 * 1024 * 1024   # 10 MB
CHUNK_SIZE          =  8 * 1024 * 1024   #  8 MB


# ══════════════════════════════════════════════════════════════════════════════
# HELPER: tạo S3 client — dùng chung ở cả Driver và Executor
# ══════════════════════════════════════════════════════════════════════════════
def make_s3_client():
    return boto3.client(
        's3',
        endpoint_url=os.environ.get('MINIO_ENDPOINT'),
        aws_access_key_id=os.environ.get('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.environ.get('MINIO_SECRET_KEY'),
        verify=False,
        config=Config(
            connect_timeout=300,
            read_timeout=300,
            retries={'max_attempts': 3, 'mode': 'adaptive'}
        )
    )


# ══════════════════════════════════════════════════════════════════════════════
# HELPER: multipart upload — giữ nguyên 100% logic từ script gốc
# ══════════════════════════════════════════════════════════════════════════════
def upload_to_minio(s3_client, bucket, key, file_obj, file_size):
    if file_size < MULTIPART_THRESHOLD:
        file_data = file_obj.read()
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=file_data,
            ContentLength=len(file_data)
        )
    else:
        mpu   = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
        parts = []
        part_num = 1
        try:
            while True:
                chunk = file_obj.read(CHUNK_SIZE)
                if not chunk:
                    break
                resp = s3_client.upload_part(
                    Bucket=bucket, Key=key,
                    PartNumber=part_num,
                    UploadId=mpu['UploadId'],
                    Body=chunk
                )
                parts.append({'PartNumber': part_num, 'ETag': resp['ETag']})
                part_num += 1
            s3_client.complete_multipart_upload(
                Bucket=bucket, Key=key,
                UploadId=mpu['UploadId'],
                MultipartUpload={'Parts': parts}
            )
        except Exception as e:
            s3_client.abort_multipart_upload(
                Bucket=bucket, Key=key, UploadId=mpu['UploadId']
            )
            raise


# ══════════════════════════════════════════════════════════════════════════════
# DRIVER: list tất cả file cần sync theo date range
# Trả về list dict — mỗi dict là 1 row trong DataFrame
# ══════════════════════════════════════════════════════════════════════════════
def list_files_for_date_range(start_str, end_str):
    """
    Dùng PowerShell qua SSH để lấy danh sách file theo LastWriteTime.
    Giữ nguyên 100% logic từ script gốc — chỉ gom kết quả vào list thay vì
    upload ngay, để Executor xử lý song song sau.
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    file_rows   = []   # kết quả trả về cho Driver
    start_dt    = datetime.strptime(start_str, '%Y-%m-%d')
    end_dt      = datetime.strptime(end_str,   '%Y-%m-%d')
    s3_temp     = make_s3_client()   # dùng để đọc 20KB đầu file detect biz date

    try:
        logging.info(f"[Driver] Kết nối SFTP {SFTP_HOST}...")
        # look_for_keys=False + allow_agent=False: bỏ qua key negotiation,
        # force password auth thẳng → tránh Authentication timeout trên OpenSSH for Windows
        ssh.connect(
            SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASS,
            timeout=120,
            auth_timeout=120,
            banner_timeout=120,
            look_for_keys=False,
            allow_agent=False
        )
        sftp = ssh.open_sftp()

        current_dt = start_dt
        while current_dt <= end_dt:
            target_date = current_dt.strftime('%Y-%m-%d')
            logging.info(f"[Driver] Quét ngày hệ thống: {target_date}")

            ps_command = (
                f'powershell.exe -Command "'
                f'Get-ChildItem -Path \'{REMOTE_DIR}\' -File | '
                f'Where-Object {{ $_.LastWriteTime.ToString(\'yyyy-MM-dd\') -eq \'{target_date}\' }} | '
                f'ForEach-Object {{ $_.Name + \'|\' + $_.LastWriteTime.ToString(\'yyyyMMdd_HHmmss\') }}"'
            )
            _, stdout, _ = ssh.exec_command(ps_command)
            results = stdout.read().decode('utf-8').splitlines()

            if not results:
                logging.info(f"[Driver]   Không có file nào vào {target_date}.")
                current_dt += timedelta(days=1)
                continue

            for line in results:
                if '|' not in line:
                    continue
                original_name, timestamp = line.strip().split('|', 1)
                remote_path = f"{REMOTE_DIR}/{original_name}"

                try:
                    file_stat = sftp.stat(remote_path)
                    file_size = file_stat.st_size

                    # Đọc 20KB đầu để detect business date — giữ nguyên logic gốc
                    with sftp.open(remote_path, 'rb') as fh:
                        raw_head = fh.read(20000)

                    text        = raw_head.decode('latin-1', 'ignore').upper()
                    date_match  = re.search(
                        r"AS AT CLOSE OF\s+(\d{1,2})\s+([A-Z]{3})\s+(\d{4})", text
                    )
                    matched_count = sum(1 for w in CORE_KEYWORDS if w in text)

                    if date_match and matched_count >= 5:
                        day, month_str, year = date_match.groups()
                        biz_dt           = datetime.strptime(f"{day} {month_str} {year}", "%d %b %Y")
                        formatted_biz    = biz_dt.strftime("%Y%m%d")
                        folder_biz       = biz_dt.strftime("%Y-%m-%d")
                        new_file_name    = f"{formatted_biz}_{timestamp}_{original_name}"
                        minio_key        = f"accounting/{folder_biz}/{new_file_name}"

                        file_rows.append({
                            "original_name" : original_name,
                            "remote_path"   : remote_path,
                            "minio_key"     : minio_key,
                            "folder_biz"    : folder_biz,
                            "file_size"     : file_size,
                            "timestamp"     : timestamp,
                        })
                        logging.info(f"[Driver]   Đưa vào queue: {original_name} → {minio_key}")
                    else:
                        logging.info(f"[Driver]   SKIP {original_name}: không thỏa điều kiện nội dung.")

                except Exception as e:
                    logging.error(f"[Driver]   ERR stat/detect {original_name}: {e}")

            current_dt += timedelta(days=1)

        sftp.close()
        ssh.close()
        logging.info(f"[Driver] List xong — {len(file_rows)} file đưa vào queue.")

    except Exception as e:
        logging.critical(f"[Driver] Lỗi kết nối SFTP: {e}")
        raise

    return file_rows


# ══════════════════════════════════════════════════════════════════════════════
# EXECUTOR: mapPartitions — 1 executor nhận N file, connect SFTP đúng 1 lần
# rồi loop upload toàn bộ partition, sau đó mới đóng connection.
# Trước đây dùng map() → mỗi file = 1 SSH connect/disconnect (130 file = 130x overhead).
# Nay dùng mapPartitions() → 3 executor = 3 SSH connections tổng cộng.
# ══════════════════════════════════════════════════════════════════════════════
def upload_partition_worker(rows):
    """
    Nhận iterator của toàn bộ rows trong 1 partition.
    Mở SFTP + S3 client 1 lần, upload tuần tự tất cả file trong partition,
    rồi đóng. Yield từng kết quả để Spark collect được.
    """
    rows = list(rows)   # materialise iterator — cần để đếm và retry
    if not rows:
        return

    sftp_host = os.environ.get('SFTP_HOST')
    sftp_port = int(os.environ.get('SFTP_PORT', 22))
    sftp_user = os.environ.get('SFTP_USER')
    sftp_pass = os.environ.get('SFTP_PASS')
    bucket    = os.environ.get('BUCKET_NAME')

    # ── Mở 1 SSH + SFTP connection cho cả partition ──────────────────────
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(
            sftp_host, sftp_port, sftp_user, sftp_pass,
            timeout=120, auth_timeout=120, banner_timeout=120,
            look_for_keys=False, allow_agent=False
        )
        sftp = ssh.open_sftp()
    except Exception as e:
        # Nếu không connect được thì toàn bộ partition fail
        for row in rows:
            yield f"ERROR|{row.original_name}|SFTP connect failed: {str(e)}|"
        return

    # ── Mở 1 S3 client cho cả partition ─────────────────────────────────
    s3 = boto3.client(
        's3',
        endpoint_url=os.environ.get('MINIO_ENDPOINT'),
        aws_access_key_id=os.environ.get('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.environ.get('MINIO_SECRET_KEY'),
        verify=False,
        config=Config(
            connect_timeout=300,
            read_timeout=300,
            retries={'max_attempts': 3, 'mode': 'adaptive'}
        )
    )

    # ── Loop upload tất cả file trong partition ───────────────────────────
    try:
        for row in rows:
            try:
                with sftp.open(row.remote_path, 'rb') as fh:
                    upload_to_minio(s3, bucket, row.minio_key, fh, row.file_size)
                yield f"SUCCESS|{row.original_name}|{row.minio_key}|{row.folder_biz}"
            except Exception as e:
                yield f"ERROR|{row.original_name}|{str(e)}|"
    finally:
        # Đảm bảo luôn đóng connection dù có lỗi giữa chừng
        try:
            sftp.close()
            ssh.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    spark = SparkSession.builder \
        .appName("T24_SFTP_Sync_Spark") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── Bước 1: Driver list toàn bộ file cần sync ─────────────────────────
    file_rows = list_files_for_date_range(START_DATE_STR, END_DATE_STR)

    if not file_rows:
        logging.info("[Main] Không có file nào cần sync. Kết thúc.")
        spark.stop()
        return

    # ── Bước 2: Tạo DataFrame → phân tán cho Executor upload song song ────
    schema = StructType([
        StructField("original_name", StringType(), False),
        StructField("remote_path",   StringType(), False),
        StructField("minio_key",     StringType(), False),
        StructField("folder_biz",    StringType(), False),
        StructField("file_size",     LongType(),   False),
        StructField("timestamp",     StringType(), False),
    ])

    files_df = spark.createDataFrame(file_rows, schema=schema)

    # Repartition: số partition = số executor instances
    # → mỗi executor nhận ~(total_files / num_executors) file
    # → share 1 SFTP connection per executor thay vì 1 connection per file
    num_executors = int(os.environ.get('SPARK_EXECUTOR_INSTANCES', 3))
    num_partitions = min(num_executors, len(file_rows))
    files_df = files_df.repartition(num_partitions)

    logging.info(f"[Main] Bắt đầu upload song song {len(file_rows)} file "
                 f"trên {num_partitions} partitions ({num_executors} executors)...")
    logging.info(f"[Main] Mỗi executor xử lý ~{len(file_rows)//num_partitions} file / 1 SFTP connection")
    sync_results = files_df.rdd.mapPartitions(upload_partition_worker).collect()

    # ── Bước 3: Driver log kết quả + tạo _SUCCESS markers ─────────────────
    success_folders = set()
    error_count     = 0

    for result in sync_results:
        parts = result.split('|', 3)
        status = parts[0]
        if status == 'SUCCESS':
            _, original_name, minio_key, folder_biz = parts
            logging.info(f"  [OK]  {original_name} → {minio_key}")
            success_folders.add(folder_biz)
        else:
            _, original_name, error_msg, _ = parts
            logging.error(f"  [ERR] {original_name}: {error_msg}")
            error_count += 1

    # Tạo _SUCCESS file cho mỗi biz date folder đã upload thành công
    if success_folders:
        s3_driver = make_s3_client()
        for folder in sorted(success_folders):
            success_key = f"accounting/{folder}/_SUCCESS"
            try:
                s3_driver.put_object(Bucket=BUCKET_NAME, Key=success_key, Body=b'')
                logging.info(f"  [SUCCESS marker] {success_key}")
            except Exception as e:
                logging.error(f"  [ERR _SUCCESS] {folder}: {e}")

    # ── Summary ────────────────────────────────────────────────────────────
    logging.info("=" * 60)
    logging.info(f"SYNC HOÀN TẤT")
    logging.info(f"  Tổng file queue  : {len(file_rows)}")
    logging.info(f"  Upload thành công: {len(file_rows) - error_count}")
    logging.info(f"  Lỗi              : {error_count}")
    logging.info(f"  Biz date folders : {sorted(success_folders)}")
    logging.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()