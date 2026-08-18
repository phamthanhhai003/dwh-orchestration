"""
Parse Provision Extract File & Map to DFE Template Headers
=============================================================
Input  : Bnctl_Loan_Provision_YYYYMMDD.csv  (pipe-delimited, no header row)
Output : Provision_Mapped_YYYYMMDD.csv  (with DFE template column headers)

CSV Column → DFE Field Position → Field Name mapping
------------------------------------------------------
col[0]  → Pos 2  → Account Number       (@ID – duplicate of col[1])
col[1]  → Pos 2  → Account Number       (@ID)
col[2]  → Pos 3  → Category             (numeric code)
col[3]  → Pos 4  → Product Name
col[4]  → Pos 5  → Customer Number
col[5]  → Pos 7  → Account Officer      (raw / may be empty)
col[6]  → Pos 6  → Customer Name
col[7]  → Pos 8  → Currency
col[8]  → Pos 9  → Auto Class
col[9]  → Pos 10 → Loan Outstanding
col[10] → Pos 11 → Posted Provision
col[11] → Pos 12 → Provision Type
col[12] → Pos 13 → Base Amount
col[13] → Pos 14 → Calculated Provision
col[14] → Pos 15 → Manual Provision     (trailing field)
"""

import csv
import os
import re
import boto3
import io
from pathlib import Path

def get_s3_client():
    endpoint = os.getenv('MINIO_ENDPOINT')
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
        use_ssl=False
    )

DELIMITER   = "|"

# Map: output column name  →  index in the raw pipe-delimited row
COLUMN_MAP = {
    "Account Number"     : 1,   # @ID  (col 0 == col 1, use col 1)
    "Category"           : 2,   # numeric product-category code
    "Product Name"       : 3,   # short product name
    "Customer Number"    : 4,   # CUSTOMER id
    "Customer Name"      : 6,   # CUSTOMER NAME.1 / SHORT.NAME
    "Account Officer"    : 5,   # DEPT.ACCT.OFFICER (often empty in raw file)
    "Currency"           : 7,   # CURRENCY
    "Auto Class"         : 8,   # AUTO CLASS (e.g. STANDARD, LOSS …)
    "Loan Outstanding"   : 9,   # WORKING BALANCE
    "Posted Provision"   : 10,  # POST.PROV.AMT
    "Provision Type"     : 11,  # CALC.PROV.TYPE
    "Base Amount"        : 12,  # CALC.BASE.AMT
    "Calculated Provision": 13, # CALC.PROV.AMT
    "Manual Provision"   : 14,  # MAN.PROV.AMT (trailing field)
}

NUMERIC_COLUMNS = {
    "Loan Outstanding",
    "Posted Provision",
    "Base Amount",
    "Calculated Provision",
    "Manual Provision",
}




def parse_value(raw: str, col_name: str) -> str:
    """
    Clean and convert a single raw field value.
    Numeric columns: strip whitespace; keep empty as empty string.
    """
    value = raw.strip()
    if col_name in NUMERIC_COLUMNS:
        # Keep empty as-is; validate numeric format
        if value and not re.match(r"^-?\d+(\.\d+)?$", value):
            print(f"  ⚠  Non-numeric value in '{col_name}': '{value}'")
    return value


def parse_provision_content(file_content: str, target_date: str) -> str:
    headers = list(COLUMN_MAP.keys())
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()
    
    rows_ok = 0
    rows_err = 0
    
    # Để validation: lưu dòng đầu tiên để kiểm tra
    first_row_data = None
    
    for line_no, raw_line in enumerate(file_content.splitlines(), start=1):
        raw_line = raw_line.rstrip("\n")
        if not raw_line.strip():
            continue

        fields = raw_line.split(DELIMITER)

        # Expect at least 14 pipe-separated values
        if len(fields) < 14:
            print(f"  ⚠  Line {line_no}: only {len(fields)} fields – skipped.")
            rows_err += 1
            continue

        row = {
            col: parse_value(
                fields[idx] if idx < len(fields) else "",
                col
            )
            for col, idx in COLUMN_MAP.items()
        }

        # Lưu dòng đầu để validation
        if first_row_data is None:
            first_row_data = {
                'raw_fields': fields,
                'mapped_row': row
            }

        writer.writerow(row)
        rows_ok += 1

    print(f"✅  Processed {rows_ok} rows")
    if rows_err:
        print(f"⚠   {rows_err} rows skipped due to parsing errors.")
    
    # Validation: hiển thị mapping của dòng đầu tiên
    if first_row_data:
        print(f"\n📋 VALIDATION - Dòng đầu tiên:")
        print(f"Raw data có {len(first_row_data['raw_fields'])} fields:")
        for i, value in enumerate(first_row_data['raw_fields'][:15]):  # Hiển thị 15 field đầu
            print(f"  Field[{i:2d}]: '{value}'")
        
        print(f"\nMapped data:")
        for col, value in list(first_row_data['mapped_row'].items())[:10]:  # Hiển thị 10 cột đầu
            original_idx = COLUMN_MAP[col]
            original_value = first_row_data['raw_fields'][original_idx] if original_idx < len(first_row_data['raw_fields']) else "N/A"
            print(f"  {col:<20}: '{value}' (từ field[{original_idx}]: '{original_value}')")
    
    return output.getvalue()


def main():
    s3 = get_s3_client()
    raw_bucket, bronze_bucket = 'raw', 'bronze'
    
    target_date = os.getenv('PROCESS_DATE')
    if target_date:
        input_prefix = f"credit/{target_date}/"
        print(f"--- Đang xử lý riêng cho ngày: {target_date} ---")
    else:
        input_prefix = "credit/"
        print(f"--- Quét toàn bộ folder credit/ ---")

    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=raw_bucket, Prefix=input_prefix):
            if 'Contents' not in page: continue

            for obj in page['Contents']:
                raw_key = obj['Key']
                if raw_key.endswith(('_SUCCESS', '/')): continue
                
                file_name = os.path.basename(raw_key).upper()
                if "PROVISION" not in file_name: continue
                
                file_obj = s3.get_object(Bucket=raw_bucket, Key=raw_key)
                raw_data = file_obj['Body'].read()
                
                file_content = None
                for enc in ['utf-8', 'cp1252', 'latin1']:
                    try:
                        file_content = raw_data.decode(enc)
                        break
                    except: continue
                
                if not file_content: continue

                # Parse provision content
                parsed_csv = parse_provision_content(file_content, target_date)
                
                bronze_key = f"credit_parsed/provision/{target_date}/{os.path.basename(raw_key)}"
                s3.put_object(
                    Bucket=bronze_bucket,
                    Key=bronze_key,
                    Body=parsed_csv.encode('utf-8-sig')
                )
                print(f"OK: {os.path.basename(raw_key)} -> {bronze_key}")

    except Exception as e:
        print(f"Lỗi: {e}")

if __name__ == "__main__":
    main()