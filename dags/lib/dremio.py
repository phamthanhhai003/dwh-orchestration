"""Dremio REST helpers dùng chung cho orchestration DAG (cob_gate, dbt gate...).

Đọc Iceberg/Bronze + etl_stream_progress + table_snapshot qua Dremio REST.
Credentials: Airflow Variables dremio_host / dremio_user / dremio_password.
Tách từ cob_daily_pipeline_dag (PoC cũ) — giữ nguyên cách dùng urllib (đã chạy ổn).
"""
from __future__ import annotations

import json
import time
import urllib.request

from airflow.models import Variable


def login() -> tuple[str, str]:
    host = Variable.get("dremio_host")
    base = f"http://{host}:9047"
    r = urllib.request.urlopen(urllib.request.Request(
        base + "/apiv2/login",
        data=json.dumps({"userName": Variable.get("dremio_user"),
                         "password": Variable.get("dremio_password")}).encode(),
        headers={"Content-Type": "application/json"}))
    return base, json.load(r)["token"]


def query(base: str, tok: str, sql: str, tries: int = 40) -> list[dict]:
    jid = json.load(urllib.request.urlopen(urllib.request.Request(
        base + "/api/v3/sql", data=json.dumps({"sql": sql}).encode(),
        headers={"Authorization": "_dremio" + tok,
                 "Content-Type": "application/json"})))["id"]
    st = {}
    for _ in range(tries):
        time.sleep(2)
        st = json.load(urllib.request.urlopen(urllib.request.Request(
            base + f"/api/v3/job/{jid}",
            headers={"Authorization": "_dremio" + tok})))
        if st["jobState"] in ("COMPLETED", "FAILED", "CANCELED"):
            break
    if st.get("jobState") != "COMPLETED":
        raise RuntimeError(f"dremio {st.get('jobState')}: {st.get('errorMessage')}")
    return json.load(urllib.request.urlopen(urllib.request.Request(
        base + f"/api/v3/job/{jid}/results?offset=0&limit=500",
        headers={"Authorization": "_dremio" + tok}))).get("rows", [])


def refresh(base: str, tok: str, table: str) -> None:
    """REFRESH METADATA (Iceberg) — bỏ qua lỗi (bảng chưa có metadata)."""
    try:
        query(base, tok, f"ALTER TABLE {table} REFRESH METADATA")
    except Exception:
        pass


def first(v):
    """Dremio trả cột array đôi khi bọc list — lấy phần tử đầu."""
    return v[0] if isinstance(v, list) and v else v


def latest_snapshot_id(base: str, tok: str, table: str) -> str:
    """snapshot_id mới nhất của bảng Iceberg (để pin AT SNAPSHOT)."""
    refresh(base, tok, table)
    rows = query(base, tok,
                 f"SELECT snapshot_id FROM TABLE(table_snapshot('{table}')) "
                 f"ORDER BY committed_at DESC LIMIT 1")
    if not rows:
        raise RuntimeError(f"không lấy được snapshot_id của {table}")
    return str(first(rows[0]["snapshot_id"]))
