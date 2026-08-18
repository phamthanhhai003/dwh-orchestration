"""
Treasury Remittance validator — Pattern A (vendor-organized folder, daily).

SFTP structure:
    {REMOTE_DIR_TREASURY}/{YYYY-MM-DD}/BNCTL_FT_INREMIT_{YYYY-MM-DD}.xlsx

Validation rules:
  - Folder {YYYY-MM-DD} tồn tại
  - File BNCTL_FT_INREMIT_*.xlsx tồn tại (naming flexible — có thể có suffix/prefix)
  - File không rỗng (≥ 2KB — Excel thường ≥ vài KB dù rỗng data)
  - Valid xlsx (có workbook.xml) với ≥1 sheet
  - Sheet đầu có ≥ 17 cột (parser đọc cols 0-16)
"""

import re

from .base import (
    SFTPContext,
    SFTPValidator,
    ValidationResult,
    xlsx_first_sheet_column_count,
    xlsx_sheet_names,
)


# Parser treasury_remittance_parser_spark.py main row có 17 cột (indices 0-16)
MIN_COLUMNS = 17
FILE_PATTERN = re.compile(r"BNCTL_FT_INREMIT.*\.xlsx$", re.IGNORECASE)


class TreasuryValidator(SFTPValidator):
    NAME = "treasury_remittance"
    REMOTE_DIR_VAR = "SFTP_REMOTE_DIR_TREASURY"

    def validate_batch(self, sftp: SFTPContext, result: ValidationResult) -> None:
        if not self.cob:
            result.fail("cob parameter is required for treasury validator")
            return

        # 1. Folder
        if not sftp.folder_exists(self.cob):
            result.fail(f"Folder not found: {self.cob}")
            return

        files = sftp.list_folder(self.cob)
        result.set_metric("total_files_in_folder", len(files))

        # 2. Match file required
        matched = [f for f in files if FILE_PATTERN.match(f.filename)]
        result.set_metric("matched_files", [f.filename for f in matched])

        if not matched:
            result.fail(
                f"No treasury file matching BNCTL_FT_INREMIT_*.xlsx in {self.cob}/. "
                f"Files found: {[f.filename for f in files]}"
            )
            return

        if len(matched) > 1:
            result.warn(
                f"Multiple treasury files: {[f.filename for f in matched]}. "
                f"Parser sẽ xử lý tất cả."
            )

        # 3. Download & inspect (xlsx usually < 10MB)
        for attr in matched:
            rel_path = f"{self.cob}/{attr.filename}"

            if attr.st_size < 2000:
                result.fail(
                    f"{attr.filename}: xlsx too small ({attr.st_size} bytes) — "
                    f"có thể corrupt hoặc empty"
                )
                continue

            xlsx_bytes = sftp.read_full(rel_path)
            if not xlsx_bytes:
                result.fail(f"{attr.filename}: cannot download")
                continue

            sheets = xlsx_sheet_names(xlsx_bytes)
            if not sheets:
                result.fail(
                    f"{attr.filename}: not a valid xlsx (no sheets found)"
                )
                continue

            cols = xlsx_first_sheet_column_count(xlsx_bytes)
            if cols is None:
                result.fail(f"{attr.filename}: cannot inspect first sheet")
                continue

            if cols < MIN_COLUMNS:
                result.fail(
                    f"{attr.filename}: first sheet has only {cols} cols "
                    f"(expect ≥{MIN_COLUMNS})"
                )
            else:
                result.set_metric(f"{attr.filename}_sheets", sheets)
                result.set_metric(f"{attr.filename}_first_sheet_cols", cols)
