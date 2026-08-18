import os
import io
import csv
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

spark = (
    SparkSession.builder
    .appName("BNK to AML Parsed")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint",                  MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",                MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",                MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access",         "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled",    "true")
    .config("spark.hadoop.fs.s3a.impl",                      "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",  "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        use_ssl=True,
    )


def get_category(fname):
    u = fname.upper()
    if "ACCOUNT"  in u:                return "account"
    if "CUSTOMER" in u:                return "customer"
    if "TXN_ENTRY" in u or "TXN" in u: return "txn_entry"
    return "others"


def decode(raw):
    for enc in ["utf-16", "utf-8", "cp1252", "latin1"]:
        try:    return raw.decode(enc)
        except: pass
    return None


def parse(content):
    headers, rows = None, []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        fields = line.replace("}", ": ").split("~")
        if headers is None: headers = fields
        else:               rows.append(fields)
    return headers, rows


def process_file(raw_key):
    s3        = get_s3()
    file_name = os.path.basename(raw_key)
    category  = get_category(file_name)

    raw_data     = s3.get_object(Bucket="raw", Key=raw_key)["Body"].read()
    file_content = decode(raw_data)
    if not file_content:
        return f"ERROR decode: {raw_key}"

    headers, rows = parse(file_content)
    if not headers:
        return f"ERROR no header: {raw_key}"

    buf = io.StringIO()
    w   = csv.writer(buf)
    w.writerow(headers)
    w.writerows(rows)
    csv_bytes = buf.getvalue().encode("utf-8-sig")

    out_key = f"aml_parsed/{category}/{file_name}"
    s3.upload_fileobj(
        io.BytesIO(csv_bytes),
        "raw",
        out_key,
        ExtraArgs={"ContentType": "text/csv"},
    )

    return f"OK: {file_name} -> raw/{out_key} ({len(rows)} dòng)"


def main():
    s3        = get_s3()
    raw_keys  = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket="raw", Prefix="bnk/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(("_SUCCESS", "/")):
                raw_keys.append(key)

    if not raw_keys:
        print("Không tìm thấy file nào trong raw/bnk/")
        return

    print(f"Tìm thấy {len(raw_keys)} file: {raw_keys}")

    # chạy song song bằng thread, tránh hoàn toàn pickle issue của Spark RDD
    with ThreadPoolExecutor(max_workers=len(raw_keys)) as pool:
        futures = {pool.submit(process_file, k): k for k in raw_keys}
        for future in as_completed(futures):
            print(future.result())


if __name__ == "__main__":
    main()