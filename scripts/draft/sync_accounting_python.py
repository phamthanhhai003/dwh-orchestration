import paramiko
import os
import boto3
import urllib3
import logging
import re 
from datetime import datetime, timedelta
from botocore.config import Config
import sys

# Tắt cảnh báo SSL cho MinIO nội bộ
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout) # Ép đẩy log ra stream mà Airflow đang đọc
    ],
    force=True
)

# --- CẤU HÌNH HỆ THỐNG (Lấy từ Environment Variables) ---
SFTP_HOST = os.environ.get('SFTP_HOST')
SFTP_PORT = int(os.environ.get('SFTP_PORT', 22))
SFTP_USER = os.environ.get('SFTP_USER')
SFTP_PASS = os.environ.get('SFTP_PASS')
REMOTE_DIR = os.environ.get('REMOTE_DIR')
BUCKET_NAME = os.environ.get('BUCKET_NAME')

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')

START_DATE_STR = os.environ.get('START_DATE')
END_DATE_STR = os.environ.get('END_DATE')

# Danh sách từ khóa để lọc nội dung báo cáo chuẩn
CORE_KEYWORDS = ["LINE", "DESCRIPTION", "OPENING", "DEBIT", "CREDIT", "CLOSING", "BALANCE"]

MULTIPART_THRESHOLD = 10 * 1024 * 1024  # 10MB
CHUNK_SIZE = 8 * 1024 * 1024           # 8MB chunks

def upload_to_minio(s3_client, bucket, key, file_obj, file_size):
    """Upload file lên MinIO - hỗ trợ Multipart cho file lớn"""
    if file_size < MULTIPART_THRESHOLD:
        file_data = file_obj.read()
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=file_data,
            ContentLength=len(file_data)
        )
    else:
        mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
        parts = []
        part_num = 1
        try:
            while True:
                chunk = file_obj.read(CHUNK_SIZE)
                if not chunk:
                    break
                response = s3_client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_num,
                    UploadId=mpu['UploadId'],
                    Body=chunk
                )
                parts.append({
                    'PartNumber': part_num,
                    'ETag': response['ETag']
                })
                part_num += 1
            s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=mpu['UploadId'],
                MultipartUpload={'Parts': parts}
            )
        except Exception as e:
            s3_client.abort_multipart_upload(
                Bucket=bucket, Key=key, UploadId=mpu['UploadId']
            )
            raise e

def sync_t24_to_minio():
    s3_config = Config(
        connect_timeout=300,
        read_timeout=300,
        retries={'max_attempts': 3, 'mode': 'adaptive'}
    )
    
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        verify=False,
        config=s3_config
    )

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        logging.info(f"[*] Kết nối tới Windows Server {SFTP_HOST}...")
        client.connect(SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASS, timeout=120, auth_timeout=120)
        sftp = client.open_sftp()

        start_dt = datetime.strptime(START_DATE_STR, '%Y-%m-%d')
        end_dt = datetime.strptime(END_DATE_STR, '%Y-%m-%d')
        current_dt = start_dt

        while current_dt <= end_dt:
            target_date = current_dt.strftime('%Y-%m-%d')
            logging.info(f"--- Đang xử lý ngày quét hệ thống: {target_date} ---")
            
            # BIẾN THEO DÕI: Lưu các folder business date thực tế có dữ liệu được upload
            uploaded_biz_folders = set()

            ps_command = (
                f'powershell.exe -Command "'
                f'Get-ChildItem -Path \'{REMOTE_DIR}\' -File | '
                f'Where-Object {{ $_.LastWriteTime.ToString(\'yyyy-MM-dd\') -eq \'{target_date}\' }} | '
                f'ForEach-Object {{ $_.Name + \'|\' + $_.LastWriteTime.ToString(\'yyyyMMdd_HHmmss\') }}"'
            )
            
            stdin, stdout, stderr = client.exec_command(ps_command)
            results = stdout.read().decode('utf-8').splitlines()
            
            if not results:
                logging.info(f"    [!] Không tìm thấy file nào được tạo vào ngày {target_date}.")
            else:
                for line in results:
                    if '|' not in line: continue
                    original_name, timestamp = line.strip().split('|')
                    remote_path = f"{REMOTE_DIR}/{original_name}"

                    try:
                        file_stat = sftp.stat(remote_path)
                        file_size = file_stat.st_size
                        
                        with sftp.open(remote_path, 'rb') as remote_file:
                            raw_head = remote_file.read(20000)
                            text = raw_head.decode('latin-1', 'ignore').upper()
                            
                            # Regex tìm Business Date bên trong nội dung file
                            date_match = re.search(r"AS AT CLOSE OF\s+(\d{1,2})\s+([A-Z]{3})\s+(\d{4})", text)
                            matched_count = sum(1 for word in CORE_KEYWORDS if word in text)
                            
                            if date_match and matched_count >= 5:
                                day, month_str, year = date_match.groups()
                                biz_dt = datetime.strptime(f"{day} {month_str} {year}", "%d %b %Y")
                                formatted_biz_date = biz_dt.strftime("%Y%m%d")
                                folder_biz_date = biz_dt.strftime("%Y-%m-%d")

                                # Định nghĩa đường dẫn MinIO
                                new_file_name = f"{formatted_biz_date}_{timestamp}_{original_name}"
                                minio_path = f"accounting_full/{folder_biz_date}/{new_file_name}"

                                # Thực hiện upload dữ liệu
                                remote_file.seek(0)
                                upload_to_minio(s3_client, BUCKET_NAME, minio_path, remote_file, file_size)
                                logging.info(f"    [OK] BizDate: {formatted_biz_date} -> {minio_path}")
                                
                                # Đánh dấu folder này đã có dữ liệu mới
                                uploaded_biz_folders.add(folder_biz_date)
                            else:
                                logging.info(f"    [SKIP] {original_name}: Không thỏa mãn điều kiện nội dung.")
                                
                    except Exception as e:
                        logging.error(f"    [ERR] Lỗi xử lý file {original_name}: {str(e)}")
            
            # --- LOGIC ĐẨY FILE _SUCCESS ---
            # Sau khi duyệt hết tất cả file của target_date, tạo file _SUCCESS cho các folder tương ứng
            for folder in uploaded_biz_folders:
                success_key = f"accounting_full/{folder}/_SUCCESS"
                try:
                    s3_client.put_object(Bucket=BUCKET_NAME, Key=success_key, Body=b'')
                    logging.info(f"    [SUCCESS] Đã tạo file đánh dấu hoàn tất cho folder: {folder}")
                except Exception as e:
                    logging.error(f"    [ERR] Không thể tạo file _SUCCESS tại {folder}: {e}")

            current_dt += timedelta(days=1)

        sftp.close()
        client.close()
        logging.info("[+] TẤT CẢ TIẾN TRÌNH ĐÃ HOÀN TẤT.")
        
    except Exception as e:
        logging.critical(f"[X] LỖI HỆ THỐNG: {e}")

if __name__ == "__main__":
    sync_t24_to_minio()