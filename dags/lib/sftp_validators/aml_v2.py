"""
AML V2 validator — Pattern A (vendor-organized folder, daily).

SFTP structure:
    {REMOTE_DIR_AML}/{YYYY-MM-DD}/
        9k_original.xlsx
        AML03_original.xlsx
        AML05_original.xlsx
        AOT_original.xlsx
        FCM_original.xlsx

Parser là Python subprocess (không Spark), đọc toàn bộ file xlsx để parse.

Validation rules:
  - Folder {YYYY-MM-DD} tồn tại
  - 5 file required tồn tại (naming strict)
  - Mỗi file ≥ 2KB (xlsx rỗng thường ~5KB trở lên)
  - Valid xlsx format (có workbook.xml)

Không check sâu column count vì parser xử lý từng loại report khác nhau,
mỗi loại có schema riêng — để parser tự validate runtime.
"""

import re

from .base import (
    SFTPContext,
    SFTPValidator,
    ValidationResult,
    xlsx_sheet_names,
)


REQUIRED_REPORTS = {
    "9k":    re.compile(r"^9k.*\.xlsx$", re.IGNORECASE),
    "aml03": re.compile(r".*aml03.*\.xlsx$", re.IGNORECASE),
    "aml05": re.compile(r".*aml05.*\.xlsx$", re.IGNORECASE),
    "aot":   re.compile(r".*aot.*\.xlsx$", re.IGNORECASE),
    "fcm":   re.compile(r".*fcm.*\.xlsx$", re.IGNORECASE),
}

MIN_XLSX_SIZE = 2000


class AmlV2Validator(SFTPValidator):
    NAME = "aml_v2"
    REMOTE_DIR_VAR = "SFTP_REMOTE_DIR_AML"

    def validate_batch(self, sftp: SFTPContext, result: ValidationResult) -> None:
        if not self.cob:
            result.fail("cob parameter is required for aml_v2 validator")
            return

        if not sftp.folder_exists(self.cob):
            result.fail(f"Folder not found: {self.cob}")
            return

        files = sftp.list_folder(self.cob)
        if not files:
            result.fail(f"Folder {self.cob} is empty")
            return

        result.set_metric("total_files_in_folder", len(files))

        # Match từng report type
        matched: dict[str, list] = {k: [] for k in REQUIRED_REPORTS}
        for attr in files:
            for report_name, pattern in REQUIRED_REPORTS.items():
                if pattern.match(attr.filename):
                    matched[report_name].append(attr)
                    break

        # Check required + size + valid xlsx
        for report_name, found in matched.items():
            if not found:
                result.fail(f"Missing AML report: {report_name}")
                continue

            if len(found) > 1:
                result.warn(
                    f"Multiple {report_name} files: {[f.filename for f in found]}"
                )

            attr = found[0]
            if attr.st_size < MIN_XLSX_SIZE:
                result.fail(
                    f"{attr.filename}: size too small ({attr.st_size} bytes, min {MIN_XLSX_SIZE})"
                )
                continue

            rel_path = f"{self.cob}/{attr.filename}"
            xlsx_bytes = sftp.read_full(rel_path)
            if not xlsx_bytes:
                result.fail(f"{attr.filename}: cannot download")
                continue

            sheets = xlsx_sheet_names(xlsx_bytes)
            if not sheets:
                result.fail(
                    f"{attr.filename}: invalid xlsx (no sheets found)"
                )
            else:
                result.set_metric(f"{report_name}_sheets", sheets)
                result.set_metric(f"{report_name}_size", attr.st_size)
