-- Model test chứng minh E2E: đọc bronze.t24_batch ĐÃ GHIM snapshot_id (AT SNAPSHOT)
-- do gate truyền vào qua var snap_batch + business_date. Materialize ra Gold.
{{ config(materialized='table', schema='gold') }}

SELECT
    recid,
    last_run_date,
    job_status,
    CAST('{{ var("business_date") }}' AS VARCHAR) AS business_date,
    CAST('{{ var("snap_batch") }}'    AS VARCHAR) AS pinned_snapshot
FROM hive.bronze.t24_batch AT SNAPSHOT '{{ var("snap_batch") }}'
WHERE recid = 'BNK/DW.EXTRACT.EOD'
