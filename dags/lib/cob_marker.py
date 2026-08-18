"""COB-done detection — nguồn chân lý DUY NHẤT đọc marker F_BATCH.

T24 COB: record điều phối `BNK/COB.INITIALISE`, field PROCESS.STATUS (cột bronze
`process_status`, c3):  0 = COB xong · 1 = batch đang running · 2 = batch success.
COB ngày D coi như xong khi process_status về 0 (anh Cường T24 confirm 2026-06-18).

Trước đây neo `BNK/DW.EXTRACT.EOD` + job_status (c12)=0 — đổi vì EOD chỉ là 1 job lẻ
(không chắc bật/đúng mọi môi trường), còn COB.INITIALISE điều phối cả phiên COB.
Mọi DAG (cob_gate / pull_cob / dbt_model_cob) gọi hàm này — sửa rule = 1 nơi.
"""
from __future__ import annotations

from lib import dremio

COB_RECID = "BNK/COB.INITIALISE"
BATCH_TABLE = "hive.bronze.t24_batch"


def conf_override(ctx) -> tuple[str | None, str]:
    """Override D từ dag_run.conf cho replay/debug THỦ CÔNG (bỏ qua marker COB shared).

    conf {"business_date":"YYYY-MM-DD"[, "l_cob":"<lsn>"]} → (D, l_cob).
    KHÔNG có business_date → (None, "0"). l_cob mặc định "0" ⇒ caught_up qua NGAY
    (so chuỗi: mọi __lsn thật ≥ "0"; replay = stream đã chạy quá mốc cũ từ lâu, khỏi chờ).
    Caller: forced = (D is not None) → khi forced thì BỎ idempotent all_sealed (ép chạy lại).
    Trigger:  airflow dags trigger <dag> -c '{"business_date":"2026-06-25"}'
    Dùng chung cob_gate / pull_cob / sftp_hold (xem từng _detect)."""
    dr = ctx.get("dag_run")
    conf = (dr.conf or {}) if dr else {}
    cd = conf.get("business_date")
    if not cd:
        return None, "0"
    return str(cd)[:10], str(conf.get("l_cob", "0"))


def cob_done(base: str, tok: str) -> tuple[str, str] | None:
    """Đọc marker COB.INITIALISE từ bronze t24_batch (tự refresh metadata trước).

    Trả (business_date D 'YYYY-MM-DD', l_cob = __lsn lúc COB xong) nếu process_status=0;
    ngược lại None (COB chưa xong = đang running/ status 1|2, hoặc chưa khởi tạo).
    Lưu ý số-0 tồn của COB cũ: caller phải lọc idempotent (all_sealed / is_model_built) theo D.
    """
    dremio.refresh(base, tok, BATCH_TABLE)
    rows = dremio.query(base, tok,
        f"SELECT last_run_date, process_status, \"__lsn\" AS lcob "
        f"FROM {BATCH_TABLE} WHERE recid = '{COB_RECID}'")
    if not rows:
        return None
    r = rows[0]
    if str(dremio.first(r["process_status"])) != "0":
        return None
    d = str(dremio.first(r["last_run_date"]))[:10]
    return d, str(dremio.first(r["lcob"]))
