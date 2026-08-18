"""
CRB Deposits Parser (PySpark + Iceberg)
Input : GENERAL LEDGER - BALANCE DETAILS (fixed-width text)
Output: hive.bronze.crb_deposits (Iceberg, partitioned by load_date)

Sections parsed:
  Demand   : 4310, 4320, 4340
  Time     : 4380, 4390, 4400, 4410
  Passbook : 4450, 4460, 4470, 4475, 4480, 4490, 4500, 4505, 4506, 4510, 4520
"""

import re
import sys
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit, to_date
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# MAPPING TABLES
# ─────────────────────────────────────────────

TARGET_SECTIONS: Dict[str, Tuple[str, str]] = {
    # gl_line: (section_name, deposit_type)
    "4310": ("Gouvernment",              "demand"),
    "4320": ("Business Enterprises",     "demand"),
    "4340": ("Group Account",            "demand"),
    "4380": ("Financial Institutions",   "time"),
    "4390": ("Government",               "time"),
    "4400": ("Business Enterprises",     "time"),
    "4410": ("Others",                   "time"),
    "4450": ("Passbook Savings",         "passbook"),
    "4460": ("Pledge Saving Account",    "passbook"),
    "4470": ("Elderly Savings",          "passbook"),
    "4475": ("Bolsa da Mae",             "passbook"),
    "4480": ("Deposito Asuwa'in",        "passbook"),
    "4490": ("Futuru Saving",            "passbook"),
    "4500": ("Depozitu Poupansa Emigrante", "passbook"),
    "4505": ("Depozitu Matenek",         "passbook"),
    "4506": ("Depozitu Pensionista",     "passbook"),
    "4510": ("BDM Jerasaun Foun",        "passbook"),
    "4520": ("Depozitu Fiar",            "passbook"),
}

# Group total GL lines used only for reconciliation
GROUP_TOTAL_GL = {
    "4360": "demand",
    "4430": "time",
    "4540": "passbook",
}

BRANCH_MAP: Dict[str, str] = {
    "02": "DIL", "03": "GLN", "04": "MAL", "05": "ALU",
    "06": "OCS", "07": "BCU", "08": "SAM", "09": "ANR",
    "10": "SUA", "11": "VQQ", "12": "LSP", "13": "LQC", "14": "MTT",
}

PRODUCT_MAP: Dict[str, str] = {
    "1001": "Business Enterprises",
    "1002": "Government",
    "1003": "Group Account",
    "1190": "Time Deposit",
    "6005": "Passbook Savings",
    "6006": "Futuru Saving",
    "6007": "Elderly Savings",
    "6008": "Pledge Saving",
    "6009": "Pledge Saving",
    "6010": "Depozitu Poupansa Emigrante",
    "6011": "Bolsa da Mae",
    "6012": "Deposito Asuwa'in",
    "6013": "Depozitu Matenek",
    "6014": "Depozitu Pensionista",
    "6015": "BDM Jerasaun Foun",
    "6016": "Depozitu Fiar",
}

# ─────────────────────────────────────────────
# REGEX
# ─────────────────────────────────────────────

SECTION_HEAD_RE = re.compile(r'^(\d{4})\s+([A-Za-z\'(].+?)\s*$')
# File totals có thể ÂM (số dư tiền gửi = liability, T24 in dấu '-'). Phải bắt '-?'
# để khớp dấu với data rows (_D đã có -?); thiếu → file_total dương, lệch 2× → recon FAIL.
TOTAL_FOR_RE    = re.compile(r'^TOTAL FOR (\d{4})\b.+?(-?[\d,]+\.\d{2})\s*$')
GROUP_TOTAL_RE  = re.compile(r'^(\d{4})\s+Total\s.+?(-?[\d,]+\.\d{2})\s*$')

# Data row: account  USD  customer_name  ccy_amt  local_amt  [int_rate int_type value_date mat_date]
_D     = r'(-?[\d,]+\.\d{2})'
_VDATE = r'([\d]+\s+[A-Z]{3}\s+\d{4})'   # VALUE DATE: T24 ordinal prefix + DD MMM YYYY
_MDATE = r'(\d{1,2}\s+[A-Z]{3}\s+\d{4})' # MAT DATE: plain DD MMM YYYY
DATA_ROW_RE = re.compile(
    r'^(\S+)\s+'                                            # account_number
    r'(USD)\s+'                                             # currency
    r'(.+?)\s{2,}'                                         # customer_name
    + _D + r'\s+' + _D                                     # currency_amt  local_ccy_amt
    + r'(?:\s+(-?[\d.]+)\s+(\S+)\s+' + _VDATE + r'\s+' + _MDATE + r')?' # optional
    + r'\s*$'
)

SKIP_TOKENS = ('PAGE NO', 'PRINTED AT', 'RE000010', '----',
               'TOTAL LINE FOR', 'OPENING BALANCE', 'DEBIT MOVEMENTS',
               'CREDIT MOVEMENTS', 'CLOSING BALANCE')

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def clean_num(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    try:
        return round(float(s.replace(',', '')), 2)
    except ValueError:
        return None

def parse_crb_date(s: Optional[str]) -> Optional[str]:
    """Parse date string — strips T24 ordinal prefix if present.
    e.g. '36021 MAR 2024' (ordinal=3602, day=1) → '2024-03-01'
         '21 MAR 2026'                           → '2026-03-21'
    """
    if not s:
        return None
    m = re.search(r'(\d{1,2})\s+([A-Z]{3})\s+(\d{4})$', s.strip())
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", '%d %b %Y').strftime('%Y-%m-%d')
        except ValueError:
            return None
    return None

def extract_account_parts(raw: str) -> Tuple[str, str, str, str]:
    """
    Returns (account_number, branch_code, branch_name, product_code).
    Strip leading zeros, then:
      - branches 10-14 → first 2 digits = branch, next 4 = product
      - branches 2-9   → first 1 digit  = branch (pad to 2), next 4 = product
    """
    acc = raw.lstrip('0') or raw
    if len(acc) < 5:
        return acc, '', 'UNKNOWN', ''

    first2 = acc[:2]
    if first2.isdigit() and 10 <= int(first2) <= 14:
        branch_code = first2
        product_code = acc[2:6]
    else:
        branch_code = '0' + acc[0]
        product_code = acc[1:5]

    branch_name = BRANCH_MAP.get(branch_code, f'UNKNOWN_{branch_code}')
    return acc, branch_code, branch_name, product_code

# ─────────────────────────────────────────────
# RECONCILIATION TRACKER
# ─────────────────────────────────────────────

class ReconTracker:
    def __init__(self):
        # per-section: {gl_line: {parsed_sum, parsed_rows, file_total}}
        self.sections: Dict[str, dict] = {
            gl: {"name": info[0], "type": info[1],
                 "parsed_sum": 0.0, "parsed_rows": 0, "file_total": None}
            for gl, info in TARGET_SECTIONS.items()
        }
        # group totals from file
        self.group_file_totals: Dict[str, Optional[float]] = {
            "demand": None, "time": None, "passbook": None
        }

    def add_row(self, gl_line: str, local_ccy_amt: Optional[float]):
        if gl_line in self.sections and local_ccy_amt is not None:
            self.sections[gl_line]["parsed_sum"] += local_ccy_amt
            self.sections[gl_line]["parsed_rows"] += 1

    def set_section_total(self, gl_line: str, total: float):
        if gl_line in self.sections:
            self.sections[gl_line]["file_total"] = total

    def set_group_total(self, deposit_type: str, total: float):
        self.group_file_totals[deposit_type] = total

    def run(self) -> Tuple[bool, str]:
        passed = True
        lines = []
        sep = "=" * 80

        lines.append(sep)
        lines.append("RECONCILIATION REPORT — SECTION LEVEL")
        lines.append(sep)
        lines.append(f"  {'GL':<6} {'Section':<35} {'Rows':>6} {'Parsed Sum':>18} {'File Total':>18} Status")
        lines.append("-" * 80)

        group_parsed = {"demand": 0.0, "time": 0.0, "passbook": 0.0}

        for gl in sorted(self.sections):
            s = self.sections[gl]
            group_parsed[s["type"]] += s["parsed_sum"]

            if s["file_total"] is None:
                status = "SKIP (empty)"
            else:
                diff = abs(s["parsed_sum"] - s["file_total"])
                if diff > 0.01:
                    status = f"FAIL  diff={diff:,.2f}"
                    passed = False
                else:
                    status = "OK"

            lines.append(
                f"  {gl:<6} {s['name']:<35} {s['parsed_rows']:>6} "
                f"{s['parsed_sum']:>18,.2f} "
                f"{s['file_total'] if s['file_total'] is not None else 'N/A':>18} "
                f"{status}"
            )

        lines.append(sep)
        lines.append("RECONCILIATION REPORT — GROUP LEVEL")
        lines.append("-" * 80)
        lines.append(f"  {'Type':<12} {'Parsed Sum':>18} {'File Total':>18} Status")

        for dtype in ["demand", "time", "passbook"]:
            p = group_parsed[dtype]
            f = self.group_file_totals[dtype]
            if f is None:
                status = "NO GROUP TOTAL IN FILE"
            else:
                diff = abs(p - f)
                status = "OK" if diff <= 0.01 else f"FAIL  diff={diff:,.2f}"
                if diff > 0.01:
                    passed = False
            lines.append(
                f"  {dtype.upper():<12} {p:>18,.2f} "
                f"{f if f is not None else 'N/A':>18} {status}"
            )

        lines.append(sep)
        lines.append("RECONCILIATION PASSED" if passed else "RECONCILIATION FAILED")
        lines.append(sep)

        report = "\n".join(lines)
        logger.info(report)
        return passed, report

# ─────────────────────────────────────────────
# CORE PARSER
# ─────────────────────────────────────────────

def parse_crb_content(record) -> List[Row]:
    s3_path, content = record
    source_file = s3_path.split('/')[-1]

    current_gl = None
    current_section = None
    current_dtype = None
    rows = []
    recon = ReconTracker()

    for raw in content.splitlines():
        line = raw.rstrip()
        if not line.strip():
            continue

        # ── TOTAL FOR section  (must check before SKIP) ──────────────────
        m = TOTAL_FOR_RE.match(line)
        if m:
            recon.set_section_total(m.group(1), clean_num(m.group(2)))
            continue

        # ── Group total line (4360/4430/4540) ────────────────────────────
        m = GROUP_TOTAL_RE.match(line)
        if m:
            gl = m.group(1)
            if gl in GROUP_TOTAL_GL:
                recon.set_group_total(GROUP_TOTAL_GL[gl], clean_num(m.group(2)))
            continue

        # ── Skip junk ────────────────────────────────────────────────────
        if any(t in line for t in SKIP_TOKENS):
            continue

        # ── Section header ───────────────────────────────────────────────
        m = SECTION_HEAD_RE.match(line)
        if m:
            gl = m.group(1)
            if gl in TARGET_SECTIONS:
                current_gl = gl
                current_section = TARGET_SECTIONS[gl][0]
                current_dtype = TARGET_SECTIONS[gl][1]
            else:
                current_gl = None
            continue

        # ── Data row ─────────────────────────────────────────────────────
        if current_gl is None:
            continue

        m = DATA_ROW_RE.match(line)
        if not m:
            continue

        (raw_acc, currency, customer,
         ccy_amt_s, local_amt_s,
         int_rate_s, int_type, vdate_s, mdate_s) = m.groups()

        local_amt = clean_num(local_amt_s)
        acc, branch_code, branch_name, product_code = extract_account_parts(raw_acc)

        recon.add_row(current_gl, local_amt)

        rows.append(Row(
            deposit_type     = current_dtype,
            gl_line          = current_gl,
            gl_section_name  = current_section,
            account_number   = acc,
            branch_code      = branch_code,
            branch_name      = branch_name,
            product_code     = product_code,
            product_name     = PRODUCT_MAP.get(product_code, f"UNKNOWN_{product_code}"),
            currency         = currency,
            customer_name    = customer.strip(),
            currency_amt     = clean_num(ccy_amt_s),
            local_ccy_amt    = local_amt,
            int_rate         = clean_num(int_rate_s),
            int_type         = int_type,
            value_date       = parse_crb_date(vdate_s),
            mat_date         = parse_crb_date(mdate_s),
            source_file      = source_file,
        ))

    # ── Reconcile before returning ────────────────────────────────────────
    passed, report = recon.run()
    if not passed:
        raise ValueError(f"[{source_file}] Reconciliation failed:\n{report}")

    return rows

# ─────────────────────────────────────────────
# SPARK DRIVER
# ─────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--bucket', default='raw')
    ap.add_argument('--prefix', default='crb/')
    ap.add_argument('--date',   required=True)
    ap.add_argument('--table',  default='hive.bronze.crb_deposits')
    # Default 'load_date' giữ tương thích DAG cũ; sftp_hold truyền 'business_date'
    # (khớp convention pull/sftp + reconcile_seal). Khi business_date: load_date nội-dung giữ nguyên.
    ap.add_argument('--partition-col', default='load_date')
    args = ap.parse_args()

    spark = SparkSession.builder \
        .appName(f"CRB-Deposits-Parser-{args.date}") \
        .getOrCreate()
    sc = spark.sparkContext

    input_path = f"s3a://{args.bucket}/{args.prefix}*"
    logger.info(f"Reading: {input_path}")

    schema = StructType([
        StructField("deposit_type",    StringType(), True),
        StructField("gl_line",         StringType(), True),
        StructField("gl_section_name", StringType(), True),
        StructField("account_number",  StringType(), True),
        StructField("branch_code",     StringType(), True),
        StructField("branch_name",     StringType(), True),
        StructField("product_code",    StringType(), True),
        StructField("product_name",    StringType(), True),
        StructField("currency",        StringType(), True),
        StructField("customer_name",   StringType(), True),
        StructField("currency_amt",    DoubleType(),  True),
        StructField("local_ccy_amt",   DoubleType(),  True),
        StructField("int_rate",        DoubleType(),  True),
        StructField("int_type",        StringType(), True),
        StructField("value_date",      StringType(), True),
        StructField("mat_date",        StringType(), True),
        StructField("source_file",     StringType(), True),
    ])

    rdd_files  = sc.wholeTextFiles(input_path)
    rdd_parsed = rdd_files.flatMap(parse_crb_content)

    if rdd_parsed.isEmpty():
        logger.warning("No rows parsed — nothing to write")
        spark.stop()
        sys.exit(0)

    df = spark.createDataFrame(rdd_parsed, schema=schema) \
              .withColumn(args.partition_col, to_date(lit(args.date)))

    row_count = df.count()
    logger.info(f"Writing {row_count:,} rows → {args.table}")

    if not spark.catalog.tableExists(args.table):
        df.writeTo(args.table) \
          .tableProperty("format-version", "2") \
          .partitionedBy(args.partition_col) \
          .create()
    else:
        df.writeTo(args.table).overwritePartitions()

    logger.info("Done.")
    spark.stop()

if __name__ == "__main__":
    main()
