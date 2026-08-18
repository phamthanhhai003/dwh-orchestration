"""
Operational HNF validator — Pattern A (vendor-organized folder, MONTHLY).

SFTP structure:
    {REMOTE_DIR_OPERATIONAL}/{YYYY-MM}/
        hnf/
            AccountCustomer.{YYYY-MM}.csv    (12 cols, pipe-delimited, no header)
            CRB_{YYYY-MM}.xlsx                (must have Sheet4, parser only reads Sheet4)
        agent_banking/
            TransactionsLogReport_Agency_Banking_{YYYY-MM}.xlsx  (có header)
        digital_service/
            Transaction_ATM_Local_EOM_{YYYY-MM}.csv             (có header)
            Transaction_Internet_Banking_EOM_{YYYY-MM}.xlsx     (có header)
            TransferReportBnctl_ATM_EOM_{YYYY-MM}.csv           (có header)
            TransferReportBnctl_POS_EOM_{YYYY-MM}.csv           (có header)
            TransferReportBnctl_WEB_EOM_{YYYY-MM}.csv           (có header)

Validation rules:
  - Folder {YYYY-MM} tồn tại
  - 3 subfolder tồn tại
  - 8 file required (pattern match, case-insensitive)
  - AccountCustomer.csv: ≥12 cols (schema_header trong parser)
  - CRB_*.xlsx: phải có Sheet4 (parser hardcode `'Sheet4'!A1`)
  - Các file khác: check file exists + size > threshold + (xlsx) valid format
"""

import re

from .base import (
    SFTPContext,
    SFTPValidator,
    ValidationResult,
    count_csv_columns_no_header,
    xlsx_sheet_column_count,
    xlsx_sheet_names,
)


# ── Required files per subfolder ─────────────────────────────────────────────
# Mỗi entry: (regex pattern, file_type, specific_checks)

REQUIRED_FILES = {
    "hnf": [
        {
            "name":    "account_customer",
            "pattern": re.compile(r"^AccountCustomer\..*\.csv$", re.IGNORECASE),
            "type":    "csv_no_header",
            "min_cols": 12,    # từ schema_header trong process_account_customer
            "delimiter": "|",
        },
        {
            "name":    "crb",
            "pattern": re.compile(r"^CRB_.*\.xlsx$", re.IGNORECASE),
            "type":    "xlsx_specific_sheet",
            "required_sheet": "Sheet4",
            "min_cols": 14,    # từ rename_map trong process_crb_sheet4 (indices 0..13)
        },
    ],
    "agent_banking": [
        {
            "name":    "agency_transactions",
            "pattern": re.compile(r"^TransactionsLogReport_Agency_Banking_.*\.xlsx$", re.IGNORECASE),
            "type":    "xlsx_any_sheet",
            "min_cols": 1,     # có header — chỉ check valid xlsx
        },
    ],
    "digital_service": [
        {
            "name":    "atm_local",
            "pattern": re.compile(r"^Transaction_ATM_Local_EOM_.*\.csv$", re.IGNORECASE),
            "type":    "csv_with_header",
            "min_size": 100,
        },
        {
            "name":    "internet_banking",
            "pattern": re.compile(r"^Transaction_Internet_Banking_EOM_.*\.xlsx$", re.IGNORECASE),
            "type":    "xlsx_any_sheet",
            "min_cols": 1,
        },
        {
            "name":    "transfer_atm",
            "pattern": re.compile(r"^TransferReportBnctl_ATM_EOM_.*\.csv$", re.IGNORECASE),
            "type":    "csv_with_header",
            "min_size": 100,
        },
        {
            "name":    "transfer_pos",
            "pattern": re.compile(r"^TransferReportBnctl_POS_EOM_.*\.csv$", re.IGNORECASE),
            "type":    "csv_with_header",
            "min_size": 100,
        },
        {
            "name":    "transfer_web",
            "pattern": re.compile(r"^TransferReportBnctl_WEB_EOM_.*\.csv$", re.IGNORECASE),
            "type":    "csv_with_header",
            "min_size": 100,
        },
    ],
}


class OperationalHnfValidator(SFTPValidator):
    NAME = "operational_hnf"
    REMOTE_DIR_VAR = "SFTP_REMOTE_DIR_OPERATIONAL"

    def validate_batch(self, sftp: SFTPContext, result: ValidationResult) -> None:
        if not self.month:
            result.fail("month parameter is required (format YYYY-MM)")
            return

        # 1. Month folder exists
        if not sftp.folder_exists(self.month):
            result.fail(f"Month folder not found: {self.month}")
            return

        # 2. Mỗi subfolder
        for subfolder, rules in REQUIRED_FILES.items():
            sub_path = f"{self.month}/{subfolder}"

            if not sftp.folder_exists(sub_path):
                result.fail(f"Missing subfolder: {sub_path}")
                continue

            files = sftp.list_folder(sub_path)
            if not files:
                result.fail(f"Empty subfolder: {sub_path}")
                continue

            result.set_metric(f"{subfolder}_file_count", len(files))

            # Match từng required file
            for rule in rules:
                matched = [f for f in files if rule["pattern"].match(f.filename)]

                if not matched:
                    result.fail(
                        f"Missing {subfolder}/{rule['name']}: "
                        f"pattern {rule['pattern'].pattern}"
                    )
                    continue

                if len(matched) > 1:
                    result.warn(
                        f"{subfolder}/{rule['name']}: multiple matches "
                        f"{[f.filename for f in matched]}"
                    )

                attr = matched[0]
                rel_path = f"{sub_path}/{attr.filename}"
                self._check_file(sftp, attr, rel_path, rule, result)

    def _check_file(self, sftp, attr, rel_path, rule, result):
        """Dispatcher — route tới check method theo rule.type"""
        ftype = rule["type"]

        min_size = rule.get("min_size", 500)
        if attr.st_size < min_size:
            result.fail(
                f"{attr.filename}: size too small ({attr.st_size} bytes, min {min_size})"
            )
            return

        if ftype == "csv_no_header":
            sample = sftp.read_bytes(rel_path, size=5000)
            if not sample:
                result.fail(f"{attr.filename}: cannot read")
                return
            cols = count_csv_columns_no_header(sample, delimiter=rule.get("delimiter", "|"))
            if cols < rule["min_cols"]:
                result.fail(
                    f"{attr.filename}: {cols} cols (expect ≥{rule['min_cols']})"
                )
            else:
                result.set_metric(f"{rule['name']}_cols", cols)

        elif ftype == "csv_with_header":
            # Chỉ check file đọc được, không verify schema sâu
            sample = sftp.read_bytes(rel_path, size=2000)
            if not sample or b"," not in sample and b"|" not in sample and b"\t" not in sample:
                # File không có delimiter quen thuộc → cảnh báo
                result.warn(f"{attr.filename}: no standard delimiter in first 2KB")
            result.set_metric(f"{rule['name']}_size", attr.st_size)

        elif ftype == "xlsx_specific_sheet":
            xlsx_bytes = sftp.read_full(rel_path)
            if not xlsx_bytes:
                result.fail(f"{attr.filename}: cannot download")
                return
            sheets = xlsx_sheet_names(xlsx_bytes)
            required = rule["required_sheet"]
            if required not in sheets:
                result.fail(
                    f"{attr.filename}: missing sheet '{required}'. "
                    f"Available: {sheets}"
                )
                return
            cols = xlsx_sheet_column_count(xlsx_bytes, required)
            if cols is None:
                result.fail(f"{attr.filename}: cannot inspect sheet {required}")
            elif cols < rule["min_cols"]:
                result.fail(
                    f"{attr.filename} Sheet4: {cols} cols (expect ≥{rule['min_cols']})"
                )
            else:
                result.set_metric(f"{rule['name']}_sheet4_cols", cols)

        elif ftype == "xlsx_any_sheet":
            xlsx_bytes = sftp.read_full(rel_path)
            if not xlsx_bytes:
                result.fail(f"{attr.filename}: cannot download")
                return
            sheets = xlsx_sheet_names(xlsx_bytes)
            if not sheets:
                result.fail(f"{attr.filename}: invalid xlsx (no sheets)")
            else:
                result.set_metric(f"{rule['name']}_sheets", sheets)

        else:
            result.warn(f"Unknown rule type: {ftype}")
