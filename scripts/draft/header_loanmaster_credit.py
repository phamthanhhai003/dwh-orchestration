import pandas as pd
import boto3
import os
import io

def get_s3_client():
    endpoint = os.getenv('MINIO_ENDPOINT')
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
        use_ssl=False
    )

# 1. Định nghĩa bộ khung Header (bỏ Record_ID như yêu cầu)
headers = [
    "Loan ID", "Category", "Product Name", "Customer Number", 
    "Account Officer", "Currency", "Auto Class", "Loan Outstanding", "Provision Type", 
    "Base Amount", "Commitment", "Term", "Maturity Date", "Arrangement Status", 
    "Repayment Amount", "Last Date Cr", "Opening Date", "ID Type", "Customer Name", 
    "Co Code", "Arrangement ID", "Legal ID", "Employer Name", "Interest Rate", 
    "Current Interest", "Avail Principal", "Occupation", "Customer Service Provider", 
    "Customer Telephone mobile", "Customer contact name", "Customer sign telephone number", 
    "Customer sign occupation"
]

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
                print(f"Tìm thấy file: {file_name}")
                
                # Xử lý file CRSI report (loan master)
                if "CRSIREPORT" not in file_name: 
                    print(f"  -> Bỏ qua file: {file_name}")
                    continue
                
                print(f"  -> Xử lý file: {file_name}")
                
                file_obj = s3.get_object(Bucket=raw_bucket, Key=raw_key)
                raw_data = file_obj['Body'].read()
                
                file_content = None
                for enc in ['utf-8', 'cp1252', 'latin1']:
                    try:
                        file_content = raw_data.decode(enc)
                        break
                    except: continue
                
                if not file_content: continue

                # 2. Đọc CSV với error handling và thử nhiều delimiter
                df = None
                delimiters = ['|', ',', ';', '\t']
                
                for delimiter in delimiters:
                    try:
                        print(f"    Thử delimiter '{delimiter}'...")
                        df = pd.read_csv(io.StringIO(file_content), header=None, sep=delimiter, engine='python', dtype=str)
                        print(f"    -> Thành công với delimiter '{delimiter}': {len(df.columns)} cột, {len(df)} dòng")
                        break
                    except Exception as e:
                        print(f"    -> Lỗi với delimiter '{delimiter}': {e}")
                        continue
                
                if df is None:
                    print(f"  -> Không thể đọc được file với bất kỳ delimiter nào")
                    continue
                
                # BỎ CỘT ĐẦU TIÊN (Record_ID) như yêu cầu
                print(f"    -> Bỏ cột đầu tiên (Record_ID)")
                df = df.iloc[:, 1:]  # Bỏ cột đầu tiên
                print(f"    -> Sau khi bỏ Record_ID: {len(df.columns)} cột")

                # 3. Xử lý chênh lệch số lượng cột
                num_data_cols = len(df.columns)
                num_headers = len(headers)
                
                print(f"    Data có {num_data_cols} cột, template có {num_headers} headers")
                
                if num_data_cols < num_headers:
                    # Nếu thiếu cột, chỉ lấy các header vừa vặn với số lượng cột có trong data
                    print(f"    -> Chỉ sử dụng {num_data_cols} headers đầu tiên")
                    df.columns = headers[:num_data_cols]
                elif num_data_cols > num_headers:
                    # Nếu dư cột, tạo các header tạm cho những cột bị thừa
                    print(f"    -> Tạo thêm {num_data_cols - num_headers} headers")
                    extra_headers = [f"Extra_Col_{i}" for i in range(num_headers + 1, num_data_cols + 1)]
                    df.columns = headers + extra_headers
                else:
                    df.columns = headers
                
                # 4. Validation mapping
                print("📋 VALIDATION - Mapping của dòng đầu tiên:")
                for i, col in enumerate(df.columns[:10]):  # Hiển thị 10 cột đầu
                    sample_value = df[col].iloc[0] if len(df) > 0 else "N/A"
                    print(f"  Col[{i:2d}] '{col}': '{sample_value}'")
                
                print(f"\n✅ Đã map thành công! Preview data:")
                print(df.head())
                
                # 5. Lưu file đã parse vào bronze bucket
                output_csv = io.StringIO()
                df.to_csv(output_csv, index=False, encoding='utf-8-sig')
                
                bronze_key = f"credit_parsed/loan_master/{target_date}/{os.path.basename(raw_key)}"
                s3.put_object(
                    Bucket=bronze_bucket,
                    Key=bronze_key,
                    Body=output_csv.getvalue().encode('utf-8-sig')
                )
                print(f"OK: {os.path.basename(raw_key)} -> {bronze_key} ({len(df)} dòng)")

    except Exception as e:
        print(f"Lỗi: {e}")

if __name__ == "__main__":
    main()