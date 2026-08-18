{{
    config(
        materialized='incremental',
        unique_key=['closure_date', 'branch_no', 'account_no'],
        incremental_strategy='merge',
        database='hive',
        schema='gold'
    )
}}

-- Division: closed_account | Report type: transaction_listing
-- Template: docs/report-template/opening_and_closed.xlsx Sheet "Account Closed"
-- Columns: Branch Name, Customer Number, Account Number, Account Name,
--          Close account date, Id Doc Type, Id Doc No
-- Note: No numeric metric; row_count = CAST(1.00) synthetic column for reconciliation
-- Note: closure_date (DATE YYYY-MM-DD) is the primary date dimension; process_date is ETL snapshot

-- Adapter shaping from stg__closed_account_t24 folded inline (silver direct-read)
WITH dim_branch AS (
    SELECT co_code, branch_name
    FROM (VALUES
        ('TL0010001', 'BNK'),
        ('TL0010002', 'DILI'),
        ('TL0010003', 'GLENU'),
        ('TL0010004', 'MALIANA'),
        ('TL0010005', 'AILEU'),
        ('TL0010006', 'OECUSSE'),
        ('TL0010007', 'BAUCAU'),
        ('TL0010008', 'SAME'),
        ('TL0010009', 'AINARO'),
        ('TL0010010', 'SUAI'),
        ('TL0010011', 'VIQUEQUE'),
        ('TL0010012', 'LOSPALOS'),
        ('TL0010013', 'LISQUICA'),
        ('TL0010014', 'MANATUTO')
    ) AS t(co_code, branch_name)
),

silver AS (
    SELECT
        TRIM(CAST(CAST(s.close_date AS DATE) AS VARCHAR))                                   AS closure_date,
        TRIM(CAST(s.branch_code AS VARCHAR))                                                 AS branch_no,
        TRIM(CAST(s.id AS VARCHAR))                                                          AS account_no,
        COALESCE(b.branch_name, TRIM(CAST(s.branch_code AS VARCHAR)))                       AS branch_name,
        TRIM(CAST(s.customer AS VARCHAR))                                                    AS customer_id,
        -- "Account Name": ACCOUNT$HIS ACCOUNT.TITLE.1/.2 (silver.acct_title). Populated for the
        -- 7,332/100,000 closed accounts that carry a $HIS image; rest NULL (honest $HIS gap).
        NULLIF(TRIM(CAST(s.acct_title AS VARCHAR)), '')                                      AS cust_name,
        -- Id Doc Type / Id Doc No live in CUSTOMER (LEGAL.ID), which is NOT a table of the
        -- EM.AC.GIC.CLOSE object (tables.md: ACCOUNT.CLOSED + ACCOUNT$HIS + POSTING.RESTRICT).
        -- No source in this ENQ -> by-design NULL.
        CAST(NULL AS VARCHAR)                                                                AS id_doc_type,
        CAST(NULL AS VARCHAR)                                                                AS id_doc_no,
        CAST(1.00 AS DECIMAL(18,2))                                                         AS row_count
    FROM hive.silver.t24_ac_gic_close s
    LEFT JOIN dim_branch b
        ON TRIM(CAST(s.branch_code AS VARCHAR)) = b.co_code
    WHERE s.close_date IS NOT NULL
    {% if var("target_date", "") != "" %}
      AND CAST(s.close_date AS DATE) = CAST('{{ var("target_date") }}' AS DATE)
    {% endif %}
),

transactions AS (
    SELECT
        closure_date,
        ROW_NUMBER() OVER (
            PARTITION BY closure_date, branch_no
            ORDER BY account_no
        )                                                                   AS row_no,
        account_no,
        branch_no,
        branch_name,
        customer_id,
        cust_name,
        id_doc_type,
        id_doc_no,
        CAST(row_count AS DECIMAL(18,2))                                    AS row_count,
        'DATA'                                                              AS row_type
    FROM silver
    WHERE 1=1
    {% if var("target_date", "") != "" %}
      AND closure_date = '{{ var("target_date") }}'
    {% endif %}
),

totals AS (
    SELECT
        closure_date,
        NULL                                                                AS row_no,
        'TOTAL'                                                             AS account_no,
        branch_no,
        NULL                                                                AS branch_name,
        NULL                                                                AS customer_id,
        NULL                                                                AS cust_name,
        NULL                                                                AS id_doc_type,
        NULL                                                                AS id_doc_no,
        CAST(SUM(row_count) AS DECIMAL(18,2))                              AS row_count,
        'TOTAL'                                                             AS row_type
    FROM transactions
    GROUP BY closure_date, branch_no
)

SELECT
    closure_date,
    row_no,
    account_no,
    branch_no,
    branch_name,
    customer_id,
    cust_name,
    id_doc_type,
    id_doc_no,
    row_count,
    row_type,
    CURRENT_TIMESTAMP                                                       AS updated_at
FROM transactions

UNION ALL

SELECT
    closure_date,
    row_no,
    account_no,
    branch_no,
    branch_name,
    customer_id,
    cust_name,
    id_doc_type,
    id_doc_no,
    row_count,
    row_type,
    CURRENT_TIMESTAMP                                                       AS updated_at
FROM totals

ORDER BY branch_no, row_type DESC, row_no
