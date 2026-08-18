import sys
import logging
import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit, to_date

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def decode_content(raw_bytes):
    for enc in ["utf-16", "utf-8", "cp1252", "latin1"]:
        try: return raw_bytes.decode(enc)
        except: continue
    return None

def parse_bnk_content(record):
    s3_path, content_bytes = record
    file_name = s3_path.split('/')[-1]
    
    content = decode_content(content_bytes)
    if not content: return []

    lines = [l.strip() for l in content.splitlines() if l.strip()]
    if not lines: return []

    try:
        headers = [h.replace("}", "").strip() for h in lines[0].split("~")]
        parsed_rows = []
        for line in lines[1:]:
            fields = line.split("~")
            if len(fields) == len(headers):
                row_dict = dict(zip(headers, fields))
                row_dict['source_file'] = file_name
                parsed_rows.append(Row(**row_dict))
        return parsed_rows
    except: return []

def process_category(spark, args, category_name, table_suffix):
    """ Hàm xử lý riêng cho từng loại dữ liệu """
    input_path = f"s3a://{args.bucket}/bnk/{args.date}/BNK_*{category_name}*"
    table_name = f"hive.bronze.aml_{table_suffix}"
    
    logger.info(f"🔍 Đang kiểm tra dữ liệu cho: {category_name}")
    
    # Đọc binary
    rdd_raw = spark.sparkContext.binaryFiles(input_path)
    if rdd_raw.isEmpty():
        logger.info(f"➖ Không tìm thấy file {category_name}. Bỏ qua.")
        return

    rdd_parsed = rdd_raw.flatMap(parse_bnk_content)
    df = spark.createDataFrame(rdd_parsed)
    df_final = df.withColumn("process_date", to_date(lit(args.date)))

    if not spark.catalog.tableExists(table_name):
        logger.info(f"✨ Tạo bảng mới: {table_name}")
        df_final.writeTo(table_name).tableProperty("format-version", "2").partitionedBy("process_date").create()
    else:
        logger.info(f"🔄 Overwrite Partition {args.date} -> {table_name}")
        df_final.writeTo(table_name).overwritePartitions()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', required=True, help="YYYY-MM-DD")
    parser.add_argument('--bucket', default='raw')
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"AML-AutoParser-{args.date}").getOrCreate()
    
    # Định nghĩa danh sách 3 category "cứng"
    categories = {
        "ACCOUNT": "account",
        "CUSTOMER": "customer",
        "TXN": "txn_entry"
    }

    # Lần lượt quét và xử lý (Nếu có file thì làm, không có thì thôi)
    for cat_name, table_suffix in categories.items():
        process_category(spark, args, cat_name, table_suffix)

    logger.info("🎉 Hoàn tất kiểm tra và xử lý toàn bộ category!")
    spark.stop()

if __name__ == "__main__":
    main()