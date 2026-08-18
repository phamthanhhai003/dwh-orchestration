"""kafka_lag — nhánh B của cob_gate wait_caught_up.

caught_up(T) = max_lsn[T] >= L_cob                       (A: bảng bận)
            OR (stream_lsn >= L_cob AND lag[T] == 0)      (B: bảng im — toàn cục qua mốc + topic cạn)

lag[topic] = kafka_end_offset − spark_committed_offset:
  - kafka_end:  KafkaConsumer.end_offsets() per topic-partition (kafka-python, gọi thẳng broker).
  - committed:  đọc checkpoint Spark structured streaming trên MinIO
                (s3a://<bucket>/<prefix>/offsets/<maxBatchId>, line2 = JSON {topic:{part:offset}}).
lag==0 ⇒ Spark đã consume hết event của topic (im/cạn). Topic chưa tồn tại (bảng rỗng) ⇒ lag 0.

(Thay bản kubectl-exec kafka-get-offsets.sh ngoài luồng — bản này client thẳng, không cần kubectl/RBAC.)
"""
from __future__ import annotations

import json

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

CKPT_BUCKET = "checkpoint"


def spark_committed(ckpt_prefix: str, minio_conn: str = "minio_conn") -> dict:
    """{topic: {partition(str): offset}} từ offsets/<maxBatchId> (line2 JSON)."""
    s3 = S3Hook(aws_conn_id=minio_conn)
    off_prefix = ckpt_prefix.rstrip("/") + "/offsets/"
    keys = s3.list_keys(bucket_name=CKPT_BUCKET, prefix=off_prefix) or []
    batches = [(int(k.rsplit("/", 1)[-1]), k) for k in keys if k.rsplit("/", 1)[-1].isdigit()]
    if not batches:
        return {}
    _, latest = max(batches)
    body = s3.read_key(latest, bucket_name=CKPT_BUCKET)
    lines = body.splitlines()
    # offsets file: line0=version(v1), line1=metadata, line2=source offsets {topic:{part:offset}}
    return json.loads(lines[2]) if len(lines) >= 3 else {}


def kafka_end_offsets(bootstrap: str, topics: list[str]) -> dict:
    """{topic: {partition(int): end_offset}}. Topic chưa tồn tại → {} (lag sẽ = 0)."""
    from kafka import KafkaConsumer, TopicPartition
    # kafka-python 3.x bỏ api_version_auto_timeout_ms (auto-detect mặc định) → KHÔNG truyền.
    c = KafkaConsumer(
        bootstrap_servers=bootstrap,
        consumer_timeout_ms=10000, request_timeout_ms=20000,
    )
    out: dict = {}
    try:
        for t in topics:
            parts = c.partitions_for_topic(t)
            if not parts:
                out[t] = {}
                continue
            tps = [TopicPartition(t, p) for p in parts]
            ends = c.end_offsets(tps)
            out[t] = {tp.partition: off for tp, off in ends.items()}
    finally:
        c.close()
    return out


def lag_per_topic(bootstrap: str, ckpt_prefix: str, topics: list[str],
                  minio_conn: str = "minio_conn") -> dict:
    """{topic: lag}; lag = Σ_part (end − committed). committed-key có thể str/int → tra cả 2."""
    committed = spark_committed(ckpt_prefix, minio_conn)
    end = kafka_end_offsets(bootstrap, topics)
    out: dict = {}
    for t in topics:
        e = end.get(t, {})
        cm = committed.get(t, {})
        out[t] = sum(e.get(p, 0) - cm.get(str(p), cm.get(p, 0)) for p in e)
    return out
