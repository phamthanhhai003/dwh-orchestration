"""
SFTP data validators for parser pipelines.

Mỗi parser DAG có 1 validator riêng (subclass của SFTPValidator) verify
folder structure, file count, naming, schema và data integrity CƠ BẢN
của data trên SFTP TRƯỚC KHI sync bắt đầu.

Usage trong DAG:

    from lib.sftp_validators import CreditValidator

    def validate_sftp_logic(**context):
        cob = context['ti'].xcom_pull(task_ids='extract_dates')['start_date']
        result = CreditValidator(cob=cob).run()
        if not result.passed:
            raise AirflowException(f"Validation failed: {result.errors}")

Framework được thiết kế để dễ mở rộng — thêm pipeline mới = tạo 1 file
validator subclass, không cần sửa base.

Xem dags/PARSER_PIPELINES.md section 10 để biết chi tiết.
"""

from .accounting import AccountingValidator
from .aml_v2 import AmlV2Validator
from .base import SFTPValidator, ValidationResult
from .bnk import BnkValidator
from .credit import CreditValidator
from .operational_hnf import OperationalHnfValidator
from .treasury import TreasuryValidator

__all__ = [
    "SFTPValidator",
    "ValidationResult",
    "AccountingValidator",
    "AmlV2Validator",
    "BnkValidator",
    "CreditValidator",
    "OperationalHnfValidator",
    "TreasuryValidator",
]
