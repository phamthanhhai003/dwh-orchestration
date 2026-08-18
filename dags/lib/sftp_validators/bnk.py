"""
BNK pipeline validator — Pattern B (T24 flat folder, daily).

SFTP structure:
    {REMOTE_DIR_BNK}/<BNK_*.csv files, flat>

File naming (T24 auto):
    BNK_ACCOUNT_<seq>.csv        — có cột MIS_DATE (canonical source)
    BNK_CUSTOMER_<seq>.csv       — có cột MIS_DATE
    BNK_AML_TXN_ENTRY_<seq>.csv  — KHÔNG có MIS_DATE (đừng validate cột này)

File format: UTF-16 hoặc UTF-8, delimiter `~`, CÓ header row.

Validation rules:
  - Root folder tồn tại
  - Phải có ít nhất 1 file mỗi category (ACCOUNT, CUSTOMER, TXN_ENTRY)
    (chunks như _3958, _3959 → nhiều file cùng category, OK)
  - BNK_ACCOUNT: header phải có cột MIS_DATE (parser canonical source)
  - BNK_CUSTOMER: header phải có cột MIS_DATE
  - Nếu cob truyền: ít nhất 1 ACCOUNT file có MIS_DATE = cob

Không check schema cột count vì file có header — parser đọc header tự động.
"""

from .base import (
    SFTPContext,
    SFTPValidator,
    ValidationResult,
    extract_biz_date_bnk,
    get_csv_header,
)


CATEGORIES = {
    "ACCOUNT":   "BNK_ACCOUNT",
    "CUSTOMER":  "BNK_CUSTOMER",
    "TXN_ENTRY": "BNK_AML_TXN_ENTRY",   # T24 có thể dùng BNK_AML_TXN_ENTRY hoặc BNK_TXN_ENTRY
}

# Cột bắt buộc trong header — chỉ check ACCOUNT + CUSTOMER (TXN_ENTRY không có MIS_DATE)
REQUIRED_HEADER_COL = "MIS_DATE"


class BnkValidator(SFTPValidator):
    NAME = "bnk"
    REMOTE_DIR_VAR = "SFTP_REMOTE_DIR_BNK"

    def validate_batch(self, sftp: SFTPContext, result: ValidationResult) -> None:
        files = sftp.list_folder("")
        if not files:
            result.fail("BNK remote folder is empty")
            return

        result.set_metric("total_files", len(files))

        # 1. Group files by category (1 file có thể match nhiều pattern nếu tên trùng)
        matched: dict[str, list] = {k: [] for k in CATEGORIES}
        for attr in files:
            fname_upper = attr.filename.upper()
            if not fname_upper.endswith(".CSV"):
                continue
            for category, prefix in CATEGORIES.items():
                # TXN_ENTRY là subset của ACCOUNT? Check chính xác bằng startswith
                if fname_upper.startswith(prefix):
                    matched[category].append(attr)
                    break

        for category, found in matched.items():
            result.set_metric(f"{category}_count", len(found))
            if not found:
                result.fail(f"Missing BNK category: {category} (expect file starting with {CATEGORIES[category]}_)")

        if not result.passed:
            return

        # 2. Check header có MIS_DATE trong ACCOUNT + CUSTOMER (không check TXN_ENTRY)
        account_biz_dates: set[str] = set()
        for category in ["ACCOUNT", "CUSTOMER"]:
            attr = matched[category][0]  # kiểm 1 file đại diện
            sample = sftp.read_bytes(attr.filename, size=10_000)
            if not sample:
                result.fail(f"{category}: cannot read {attr.filename}")
                continue

            header = get_csv_header(sample, delimiter="~")
            if not any(REQUIRED_HEADER_COL in col for col in header):
                result.fail(
                    f"{category}: header missing {REQUIRED_HEADER_COL} column. "
                    f"Header found: {header[:5]}..."
                )
                continue

            # Extract biz date cho ACCOUNT (canonical source)
            if category == "ACCOUNT":
                for account_attr in matched["ACCOUNT"]:
                    acc_sample = sftp.read_bytes(account_attr.filename, size=20_000)
                    biz = extract_biz_date_bnk(acc_sample) if acc_sample else None
                    if biz:
                        account_biz_dates.add(biz)

        result.set_metric("account_biz_dates", sorted(account_biz_dates))

        if not account_biz_dates:
            result.fail(
                "Cannot extract MIS_DATE from any ACCOUNT file. "
                "Parser sẽ fail ở phase detection."
            )
            return

        # 3. Nếu cob truyền: check match
        if self.cob and self.cob not in account_biz_dates:
            result.warn(
                f"Target COB {self.cob} not in ACCOUNT biz_dates {sorted(account_biz_dates)}. "
                f"Có thể T24 chưa EOD cho ngày này."
            )
