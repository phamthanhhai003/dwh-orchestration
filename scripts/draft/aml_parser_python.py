import csv
import os
import boto3
import io
import re

def get_s3_client():
    endpoint = os.getenv('MINIO_ENDPOINT')
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
        use_ssl=False
    )

def get_category(file_name):
    name_upper = file_name.upper()
    if "ACCOUNT" in name_upper:
        return "account", "MIS_DATE"
    elif "CUSTOMER" in name_upper:
        return "customer", "MIS_DATE"
    elif "TXN_ENTRY" in name_upper or "TXN" in name_upper:
        return "txn_entry", "operationDate"
    return "others", None

def format_date(date_str):
    if not date_str: return "unknown-date"
    clean_date = re.sub(r'[^0-9]', '', date_str)
    if len(clean_date) == 8:
        return f"{clean_date[:4]}-{clean_date[4:6]}-{clean_date[6:8]}"
    return date_str

def extract_date_from_data(file_content, target_column):
    lines = file_content.splitlines()
    if len(lines) < 2: return None
    header = lines[0].split('~')
    try:
        col_index = -1
        for i, col in enumerate(header):
            if target_column.lower() in col.lower():
                col_index = i
                break
        if col_index != -1:
            first_row = lines[1].split('~')
            if len(first_row) > col_index:
                return format_date(first_row[col_index].strip())
    except: pass
    return None

def parse_t24_content(file_content):
    char_vm, char_sm = '', ''
    output = io.StringIO()
    writer = csv.writer(output)
    lines = file_content.splitlines()
    count = 0
    for line in lines:
        line = line.strip()
        if not line: continue
        clean_line = line.replace(char_vm, ' | ').replace(char_sm, ' / ').replace('}', ': ')
        writer.writerow(clean_line.split('~'))
        count += 1
    return output.getvalue(), count

def main():
    s3 = get_s3_client()
    raw_bucket, bronze_bucket = 'raw', 'bronze'
    
    target_date = os.getenv('PROCESS_DATE')
    if target_date:
        input_prefix = f"aml/{target_date}/"
        print(f"--- Đang xử lý riêng cho ngày: {target_date} ---")
    else:
        input_prefix = "aml/"
        print(f"--- Quét toàn bộ folder aml/ ---")

    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=raw_bucket, Prefix=input_prefix):
            if 'Contents' not in page: continue

            for obj in page['Contents']:
                raw_key = obj['Key']
                if raw_key.endswith(('_SUCCESS', '/')): continue
                
                file_name = os.path.basename(raw_key)
                category, date_column = get_category(file_name)
                
                if not date_column: continue

                file_obj = s3.get_object(Bucket=raw_bucket, Key=raw_key)
                raw_data = file_obj['Body'].read()
                
                file_content = None
                for enc in ['utf-16', 'utf-8', 'cp1252', 'latin1']:
                    try:
                        file_content = raw_data.decode(enc)
                        break
                    except: continue
                
                if not file_content: continue

                detected_date = extract_date_from_data(file_content, date_column)
                final_date = detected_date if detected_date and detected_date != "unknown-date" else target_date

                clean_csv_data, row_count = parse_t24_content(file_content)
                bronze_key = f"aml_parsed/{category}/{final_date}/{file_name}"

                s3.put_object(
                    Bucket=bronze_bucket,
                    Key=bronze_key,
                    Body=clean_csv_data.encode('utf-8-sig')
                )
                print(f"OK: {file_name} -> {bronze_key} ({row_count} dòng)")

    except Exception as e:
        print(f"Lỗi: {e}")

if __name__ == "__main__":
    main()