import os
import paramiko
import boto3
import logging
from datetime import datetime
from botocore.config import Config

# ================= CONFIG =================
HOST = os.environ.get("HOST")
PORT = int(os.environ.get("PORT", 22))
USER = os.environ.get("USER")
PASS = os.environ.get("PASS")

REMOTE_DIR = os.environ.get("REMOTE_DIR")
BUCKET_NAME = os.environ.get("BUCKET_NAME")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ================= MINIO CONFIG =================
s3_config = Config(
    connect_timeout=300,
    read_timeout=300,
    retries={"max_attempts": 3, "mode": "adaptive"}
)

s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    verify=False,
    config=s3_config
)

# ===============================================

def parse_biz_date_from_filename(filename):
    try:
        parts = filename.split("_")
        biz_date_raw = parts[0]
        biz_dt = datetime.strptime(biz_date_raw, "%Y%m%d")
        return biz_dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def sync_mock_sftp_to_minio():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    uploaded_dates = set()

    try:
        logging.info(f"Connecting to SFTP {HOST}:{PORT}")
        ssh.connect(HOST, PORT, USER, PASS)
        sftp = ssh.open_sftp()

        files = sftp.listdir(REMOTE_DIR)

        if not files:
            logging.info("No files found.")
            return

        for filename in files:
            remote_path = f"{REMOTE_DIR}/{filename}"

            try:
                biz_date = parse_biz_date_from_filename(filename)
                if not biz_date:
                    logging.warning(f"Skip invalid filename: {filename}")
                    continue

                minio_key = f"accounting_full/{biz_date}/{filename}"

                file_stat = sftp.stat(remote_path)
                file_size_mb = file_stat.st_size / (1024 * 1024)

                logging.info(f"Uploading {filename} ({file_size_mb:.2f} MB)")
                logging.info(f"--> {minio_key}")

                with sftp.open(remote_path, "rb") as remote_file:
                    s3_client.upload_fileobj(remote_file, BUCKET_NAME, minio_key)

                uploaded_dates.add(biz_date)
                logging.info(f"SUCCESS: {filename}")

            except Exception as e:
                logging.error(f"ERROR processing {filename}: {e}")

        # ================= CREATE _SUCCESS =================
        for biz_date in uploaded_dates:
            success_key = f"accounting_full_mock/{biz_date}/_SUCCESS"

            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=success_key,
                Body=b""
            )

            logging.info(f"_SUCCESS created: {success_key}")

        sftp.close()
        ssh.close()

        logging.info("=== SYNC COMPLETED ===")

    except Exception as e:
        logging.critical(f"System error: {e}")


if __name__ == "__main__":
    sync_mock_sftp_to_minio()