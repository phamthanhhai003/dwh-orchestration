"""
Credit pipeline validator — Pattern A (vendor-organized folder, daily).

SFTP structure:
    {REMOTE_DIR_CREDIT}/{YYYY-MM-DD}/
        Bnctl.CrsiReport.{YYYY-MM-DD}.csv      (loan master)
        Bnctl.LoanProvision.{YYYY-MM-DD}.csv   (loan provision)
        CUSTOMERS.EXTRACT.{YYYY-MM-DD}.csv     (customer)

File format: pipe-delimited (|), KHÔNG có header row.
Parser dùng external header mapping (CUSTOMER_HEADERS, LOAN_MASTER_HEADERS,
PROVISION_COLUMN_MAP) trong credit_parser_spark.py — nên validator phải
đếm số cột row 1 để verify khớp với mapping.

Validation rules:
  - Folder {YYYY-MM-DD} tồn tại
  - 3 file required: customer / loan master / provision (pattern match, case-insensitive)
  - Mỗi file không rỗng (size > 100 bytes)
  - Số cột dòng 1 ≥ expected (từ header mapping trong parser)
"""

import re

from .base import (
    SFTPContext,
    SFTPValidator,
    ValidationResult,
    count_csv_columns_no_header,
)


# Expected min columns dựa trên DATA THẬT của vendor (data_naming/credit_data/).
# Parser `credit_parser_spark.py` có logic lenient — nếu num_data_cols < num_headers,
# chỉ dùng first N headers. Nên validator match VENDOR REALITY thay vì header count
# trong parser code (CUSTOMER_HEADERS=47, LOAN_MASTER_HEADERS=32, PROVISION max_idx=14).
#
# Khi vendor thay đổi format → chỉnh số ở đây, không cần sửa logic validator.
EXPECTED_FILES = {
    "customer": {
        "pattern":  re.compile(r"^customers?\.extract.*\.csv$", re.IGNORECASE),
        "min_cols": 42,  # vendor actual format
    },
    "loan_master": {
        "pattern":  re.compile(r"^bnctl\.crsireport.*\.csv$", re.IGNORECASE),
        "min_cols": 33,  # _c0 + 32 header fields
    },
    "loan_provision": {
        "pattern":  re.compile(r"^bnctl\.loan\.?provision.*\.csv$", re.IGNORECASE),
        "min_cols": 15,  # PROVISION_COLUMN_MAP max index 14 + 1
    },
}


class CreditValidator(SFTPValidator):
    NAME = "credit"
    REMOTE_DIR_VAR = "SFTP_REMOTE_DIR_CREDIT"

    def validate_batch(self, sftp: SFTPContext, result: ValidationResult) -> None:
        if not self.cob:
            result.fail("cob parameter is required for credit validator")
            return

        # 1. Folder ngày tồn tại
        if not sftp.folder_exists(self.cob):
            result.fail(f"Folder not found: {self.cob}")
            return

        files = sftp.list_folder(self.cob)
        if not files:
            result.fail(f"Folder {self.cob} is empty")
            return

        result.set_metric("total_files_in_folder", len(files))

        # 2. Match từng category → required file
        matched: dict[str, list] = {k: [] for k in EXPECTED_FILES}
        for attr in files:
            for category, rule in EXPECTED_FILES.items():
                if rule["pattern"].match(attr.filename):
                    matched[category].append(attr)
                    break

        # 3. Required files phải có
        for category, found in matched.items():
            if not found:
                result.fail(f"Missing required file: {category}")
            elif len(found) > 1:
                result.warn(
                    f"Multiple {category} files found: "
                    f"{[f.filename for f in found]}"
                )

        if not result.passed:
            return

        # 4. Size + schema check cho mỗi file
        for category, found in matched.items():
            if not found:
                continue
            attr = found[0]
            rel_path = f"{self.cob}/{attr.filename}"

            if attr.st_size < 100:
                result.fail(
                    f"{category} file too small: {attr.filename} "
                    f"({attr.st_size} bytes)"
                )
                continue

            # Đọc 10KB đầu, count cột (pipe-delimited, no header)
            sample = sftp.read_bytes(rel_path, size=10_000)
            if not sample:
                result.fail(f"Cannot read {category}: {attr.filename}")
                continue

            num_cols = count_csv_columns_no_header(sample, delimiter="|")
            min_cols = EXPECTED_FILES[category]["min_cols"]
            if num_cols < min_cols:
                result.fail(
                    f"{category} schema mismatch: {attr.filename} has "
                    f"{num_cols} cols (expect ≥{min_cols})"
                )
            else:
                result.set_metric(f"{category}_cols", num_cols)
                result.set_metric(f"{category}_size", attr.st_size)
