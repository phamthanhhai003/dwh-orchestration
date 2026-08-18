"""
Accounting pipeline validator — Pattern B (T24 flat folder, daily).

SFTP structure:
    {REMOTE_DIR_ACCOUNTING}/<many ASCII text files, numeric filenames>

T24 auto-exports → không kiểm tra naming. File chứa header
"AS AT CLOSE OF dd MMM yyyy" và các CORE_KEYWORDS của CRF report.

Validation rules:
  - Root folder có ít nhất MIN_FILE_COUNT file (user yêu cầu: ≥ 65)
  - Tất cả file: check format T24 CRF (AS AT CLOSE OF + ≥5/7 CORE_KEYWORDS)
  - Nếu `cob` được cung cấp: ít nhất 1 file phải có biz_date = cob

Lưu ý: accounting files không có naming chuẩn (numeric IDs từ T24),
không check per-filename. Chỉ check bulk + format content.
"""

from .base import (
    SFTPContext,
    SFTPValidator,
    ValidationResult,
    content_contains,
    count_keywords,
    extract_biz_date_accounting,
)


# Bộ keyword từ accounting_sync_spark.py CORE_KEYWORDS
CRF_CORE_KEYWORDS = [
    "LINE", "DESCRIPTION", "OPENING", "DEBIT",
    "CREDIT", "CLOSING", "BALANCE",
]
MIN_KEYWORD_MATCH = 7            # tất cả 7/7 keyword phải có → CRF hợp lệ
MIN_FILE_COUNT = 65               # User-specified: T24 EOD batch phải có ≥65 file


class AccountingValidator(SFTPValidator):
    NAME = "accounting"
    REMOTE_DIR_VAR = "SFTP_REMOTE_DIR"   # T24 accounting dùng SFTP_REMOTE_DIR mặc định

    def validate_batch(self, sftp: SFTPContext, result: ValidationResult) -> None:
        # 1. List root folder
        files = sftp.list_folder("")
        total = len(files)
        result.set_metric("total_files", total)

        if total < MIN_FILE_COUNT:
            result.fail(
                f"Accounting file count too low: {total} (expect ≥{MIN_FILE_COUNT}). "
                f"T24 EOD batch may be incomplete."
            )
            return

        # 2. Inspect all files
        valid_crf_count = 0
        biz_dates_found: set[str] = set()

        for attr in files:
            if attr.st_size < 1024:
                result.warn(f"{attr.filename}: too small ({attr.st_size} bytes), skip")
                continue

            sample = sftp.read_bytes(attr.filename, size=20_000)
            if not sample:
                result.warn(f"{attr.filename}: cannot read sample")
                continue

            # Check CRF format markers
            kw_count = count_keywords(sample, CRF_CORE_KEYWORDS)
            has_header = content_contains(sample, "AS AT CLOSE OF")

            if kw_count < MIN_KEYWORD_MATCH or not has_header:
                result.warn(
                    f"{attr.filename}: CRF format check failed "
                    f"({kw_count}/{len(CRF_CORE_KEYWORDS)} keywords, "
                    f"header={has_header}) — có thể là file không phải CRF"
                )
                continue

            valid_crf_count += 1

            # Extract biz date (content-based)
            biz = extract_biz_date_accounting(sample)
            if biz:
                biz_dates_found.add(biz)

        result.set_metric("inspected_files", total)
        result.set_metric("valid_crf_count", valid_crf_count)
        result.set_metric("biz_dates_found", sorted(biz_dates_found))

        # 3. Ít nhất 1 file hợp lệ trong toàn bộ
        if valid_crf_count == 0:
            result.fail(
                f"No valid CRF file found in all {total} files. "
                f"T24 batch format may be wrong or files corrupted."
            )
            return

        # 4. Nếu cob được truyền: verify biz_date match
        if self.cob and self.cob not in biz_dates_found:
            result.warn(
                f"Target COB {self.cob} not found in sampled biz_dates {sorted(biz_dates_found)}. "
                f"Có thể do sample chưa hit, hoặc T24 chưa có data của ngày này."
            )
