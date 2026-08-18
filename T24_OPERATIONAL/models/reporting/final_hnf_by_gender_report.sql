{{ config(
    materialized='incremental',
    unique_key=['load_date', 'row_type', 'municipal'],
    incremental_strategy='merge',
    database='hive',
    schema='gold'
) }}

-- Division: operational | Report: HNF by Gender (Table 6 — By Gender sheet)
-- Grain: 1 row per Municipal (+ TOTAL) for main table; 4 change rows for Changes section
-- row_type: 'MAIN' = current snapshot; 'NEW_ACCOUNTS' = opened in period
--   'CLOSED_TOTAL', 'CLOSED_AGE_LIMIT', 'CLOSED_OTHER' = not available (set to 0)
-- Source: stg__hnf_account_detail
-- Changes section: open_date from HNF_age_Parsed (only 2 HNF rows in current sample data)

{% set td = var('target_date', '') %}
{% if td != '' %}{% set load_dt = td %}{% else %}{% set load_dt = modules.datetime.datetime.now().strftime('%Y-%m-%d') %}{% endif %}

-- Adapter shaping from stg__hnf_account_detail_t24 folded inline (silver direct-read)
WITH dim_branch AS (
    SELECT co_code, municipal
    FROM (VALUES
        ('TL0010002', 'Dili'),
        ('TL0010003', 'Ermera'),
        ('TL0010004', 'Bobonaro'),
        ('TL0010005', 'Manatuto'),
        ('TL0010006', 'Aileu'),
        ('TL0010007', 'Oecusse'),
        ('TL0010008', 'Baucau'),
        ('TL0010009', 'Manufahi'),
        ('TL0010010', 'Ainaro'),
        ('TL0010011', 'Cova Lima'),
        ('TL0010012', 'Viqueque'),
        ('TL0010013', 'Lauten'),
        ('TL0010014', 'Liquica')
    ) AS t(co_code, municipal)
),

silver AS (
    SELECT
        TRIM(CAST(s.account_number AS VARCHAR))                                          AS account_id,
        TRIM(UPPER(CAST(s.gender  AS VARCHAR)))                                          AS gender,
        CAST(s.balance            AS DECIMAL(18,2))                                      AS balance,
        COALESCE(b.municipal, TRIM(CAST(s.company AS VARCHAR)))                          AS municipal,
        CASE
            WHEN s.opening_date IS NULL THEN NULL
            ELSE TO_CHAR(CAST(s.opening_date AS DATE), 'YYYYMMDD')
        END                                                                              AS open_date
    FROM hive.silver.t24_acct_cust s
    LEFT JOIN dim_branch b
        ON TRIM(CAST(s.company AS VARCHAR)) = b.co_code
    WHERE CAST(s.category AS INTEGER) = 6006
      AND CAST(s.balance AS DECIMAL(18,2)) > 0
),

municipal_labels AS (
    SELECT 1  AS sort_order, 'Aileu'     AS municipal UNION ALL
    SELECT 2,  'Ainaro'    UNION ALL
    SELECT 3,  'Baucau'    UNION ALL
    SELECT 4,  'Bobonaro'  UNION ALL
    SELECT 5,  'Cova Lima' UNION ALL
    SELECT 6,  'Dili'      UNION ALL
    SELECT 7,  'Ermera'    UNION ALL
    SELECT 8,  'Liquica'   UNION ALL
    SELECT 9,  'Lauten'    UNION ALL
    SELECT 10, 'Manufahi'  UNION ALL
    SELECT 11, 'Manatuto'  UNION ALL
    SELECT 12, 'Oecusse'   UNION ALL
    SELECT 13, 'Viqueque'
),

-- Main table: current snapshot by municipal × gender
main_pivoted AS (
    SELECT
        municipal,
        CAST(COUNT(CASE WHEN gender = 'MALE'   THEN account_id END) AS INTEGER)         AS male_count,
        CAST(SUM(CASE  WHEN gender = 'MALE'   THEN balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS male_amount,
        CAST(COUNT(CASE WHEN gender = 'FEMALE' THEN account_id END) AS INTEGER)         AS female_count,
        CAST(SUM(CASE  WHEN gender = 'FEMALE' THEN balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS female_amount
    FROM silver
    GROUP BY municipal
),

municipal_rows AS (
    SELECT
        CAST(TO_DATE('{{ load_dt }}', 'YYYY-MM-DD') AS VARCHAR)               AS load_date,
        'MAIN'                                                                AS row_type,
        m.sort_order,
        m.municipal,
        CAST(COALESCE(p.male_count,    0) AS INTEGER)                       AS male_count,
        CAST(COALESCE(p.male_amount,   CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS male_amount,
        CAST(COALESCE(p.female_count,  0) AS INTEGER)                       AS female_count,
        CAST(COALESCE(p.female_amount, CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS female_amount,
        CAST(COALESCE(p.male_count, 0) + COALESCE(p.female_count, 0) AS INTEGER) AS total_count,
        CAST(COALESCE(p.male_amount, CAST(0 AS DECIMAL(18,2)))
           + COALESCE(p.female_amount, CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS total_amount
    FROM municipal_labels m
    LEFT JOIN main_pivoted p ON m.municipal = p.municipal
),

total_row AS (
    SELECT
        CAST(TO_DATE('{{ load_dt }}', 'YYYY-MM-DD') AS VARCHAR)               AS load_date,
        'MAIN'                                                                AS row_type,
        99                                                                    AS sort_order,
        'TOTAL'                                                               AS municipal,
        CAST(SUM(male_count)    AS INTEGER)                                  AS male_count,
        CAST(SUM(male_amount)   AS DECIMAL(18,2))                           AS male_amount,
        CAST(SUM(female_count)  AS INTEGER)                                  AS female_count,
        CAST(SUM(female_amount) AS DECIMAL(18,2))                           AS female_amount,
        CAST(SUM(total_count)   AS INTEGER)                                  AS total_count,
        CAST(SUM(total_amount)  AS DECIMAL(18,2))                           AS total_amount
    FROM municipal_rows
),

-- Changes section: new accounts opened in the reporting period
-- period = first day of target_date month through target_date
-- open_date is in YYYYMMDD integer format in HNF_age_Parsed
new_accounts AS (
    SELECT
        CAST(TO_DATE('{{ load_dt }}', 'YYYY-MM-DD') AS VARCHAR)               AS load_date,
        'NEW_ACCOUNTS'                                                        AS row_type,
        0                                                                     AS sort_order,
        'Changes in the Period'                                               AS municipal,
        CAST(COUNT(CASE WHEN gender = 'MALE'   THEN account_id END) AS INTEGER)         AS male_count,
        CAST(SUM(CASE  WHEN gender = 'MALE'   THEN balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS male_amount,
        CAST(COUNT(CASE WHEN gender = 'FEMALE' THEN account_id END) AS INTEGER)         AS female_count,
        CAST(SUM(CASE  WHEN gender = 'FEMALE' THEN balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS female_amount,
        CAST(COUNT(CASE WHEN gender IN ('MALE','FEMALE') THEN account_id END) AS INTEGER) AS total_count,
        CAST(SUM(CASE  WHEN gender IN ('MALE','FEMALE') THEN balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS total_amount
    FROM silver
    WHERE open_date IS NOT NULL
      {% if var("target_date") != "" %}
      AND CAST(open_date AS BIGINT) >= CAST(REPLACE(SUBSTR('{{ var("target_date") }}', 1, 7), '-', '') || '01' AS BIGINT)
      AND CAST(open_date AS BIGINT) <= CAST(REPLACE('{{ var("target_date") }}', '-', '') AS BIGINT)
      {% endif %}
),

-- Closed accounts: source field not available yet — stubbed with zeros
closed_rows AS (
    SELECT
        CAST(TO_DATE('{{ load_dt }}', 'YYYY-MM-DD') AS VARCHAR)               AS load_date,
        row_type,
        0                                                                     AS sort_order,
        'Changes in the Period'                                               AS municipal,
        CAST(0 AS INTEGER)                                                   AS male_count,
        CAST(0 AS DECIMAL(18,2))                                            AS male_amount,
        CAST(0 AS INTEGER)                                                   AS female_count,
        CAST(0 AS DECIMAL(18,2))                                            AS female_amount,
        CAST(0 AS INTEGER)                                                   AS total_count,
        CAST(0 AS DECIMAL(18,2))                                            AS total_amount
    FROM (
        SELECT 'CLOSED_TOTAL'     AS row_type UNION ALL
        SELECT 'CLOSED_AGE_LIMIT' UNION ALL
        SELECT 'CLOSED_OTHER'
    ) t
),

combined AS (
    SELECT load_date, row_type, sort_order, municipal, male_count, male_amount, female_count, female_amount, total_count, total_amount FROM municipal_rows
    UNION ALL
    SELECT load_date, row_type, sort_order, municipal, male_count, male_amount, female_count, female_amount, total_count, total_amount FROM total_row
    UNION ALL
    SELECT load_date, row_type, sort_order, municipal, male_count, male_amount, female_count, female_amount, total_count, total_amount FROM new_accounts
    UNION ALL
    SELECT load_date, row_type, sort_order, municipal, male_count, male_amount, female_count, female_amount, total_count, total_amount FROM closed_rows
)

SELECT
    load_date,
    row_type,
    sort_order,
    municipal,
    male_count,
    male_amount,
    female_count,
    female_amount,
    total_count,
    total_amount,
    CURRENT_TIMESTAMP AS updated_at
FROM combined

ORDER BY load_date, row_type, sort_order
