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

# 1. Danh sách header theo thứ tự bạn đã cung cấp
# Lưu ý: Tôi đã đổi tên cột 'Legal.Id' thứ hai thành 'Legal.Id_2' để tránh trùng lặp gây lỗi trong Pandas
headers = [
    "Customer ID", 
    "Company Book", "Account Officer", "Officer Name", "Customer Type", 
    "Name 1", "Short Name", "Full Name", "Gender", "Marital Status", 
    "Language", "Nationality", "Residence", "Province/State", 
    "City/Municipal", "Suburb/Town", "Industry", "Industry Desc", 
    "Sector", "Sector Desc", "Opening Date", "Birth Date", "Age", 
    "Employ Status", "Profession", "Salary Range", "Business Type", 
    "Classification", "Cust Status", "Blocked", "Date.Time", "Legal.Id", 
    "Id.Types", "Bnctl.Pep", "Occupation", "Sp.Name", "Telmobile", 
    "Employ.Name", "Alter Id No", "Alter Id Type", "Street", 
    "Legal.Id_2", "Legal.Doc.Name", "Date of Birth", "Tel.Mobile", 
    "Phone.1", "Sms.1"
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
                if "CUSTOMER" not in file_name: continue
                
                file_obj = s3.get_object(Bucket=raw_bucket, Key=raw_key)
                raw_data = file_obj['Body'].read()
                
                file_content = None
                for enc in ['utf-8', 'cp1252', 'latin1']:
                    try:
                        file_content = raw_data.decode(enc)
                        break
                    except: continue
                
                if not file_content: continue

                # 3. Đọc file CSV với dấu phân cách là '|' (pipe)
                # Cấu hình header=None do file gốc chưa có dòng header, dtype=str để giữ số 0 đầu
                df = pd.read_csv(io.StringIO(file_content), sep='|', header=None, dtype=str)
                
                # 4. Xử lý chênh lệch số lượng cột (nếu file thực tế ít hoặc nhiều cột hơn header bạn cung cấp)
                num_data_cols = len(df.columns)
                num_headers = len(headers)
                
                if num_data_cols < num_headers:
                    # Nếu thiếu cột, chỉ lấy các header vừa vặn với số lượng cột có trong data
                    print(f"Lưu ý: Data gốc chỉ có {num_data_cols} cột, nhưng bạn cung cấp {num_headers} headers.")
                    df.columns = headers[:num_data_cols]
                elif num_data_cols > num_headers:
                    # Nếu dư cột, tạo các header tạm cho những cột bị thừa
                    print(f"Lưu ý: Data gốc có {num_data_cols} cột, bạn chỉ cung cấp {num_headers} headers.")
                    extra_headers = [f"Extra_Col_{i}" for i in range(num_headers + 1, num_data_cols + 1)]
                    df.columns = headers + extra_headers
                else:
                    df.columns = headers

                # 5. Validation mapping 
                print("📋 VALIDATION - Mapping của 5 dòng đầu:")
                print(f"Raw data structure: {len(df.columns)} cột") 
                for i, col in enumerate(df.columns[:10]):  # Hiển thị 10 cột đầu
                    sample_values = df[col].head(3).tolist()
                    print(f"  Col[{i:2d}] '{col}': {sample_values}")
                
                print("✅ Đã parse và map thành công!")
                print(df.head())
                
                # 6. Lưu file đã parse vào bronze bucket
                output_csv = io.StringIO()
                df.to_csv(output_csv, index=False, encoding='utf-8')
                
                bronze_key = f"credit_parsed/customer/{target_date}/{os.path.basename(raw_key)}"
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