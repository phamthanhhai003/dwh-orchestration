{{ config(
    materialized='incremental',
    unique_key=['load_date', 'municipal'],
    incremental_strategy='merge',
    database='hive',
    schema='gold'
) }}

-- Division: operational | Report: HNF by Age (Table 7 — BY AGE sheet)
-- Grain: 1 row per Municipal (+ TOTAL row with municipal='TOTAL'), pivoted by age group
-- Columns: count + amount for each of 4 age groups
-- Inactive/Closed columns: set to 0 — source field not available yet (open item)
-- Source: stg__hnf_account_detail at target_date

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

silver_src AS (
    SELECT
        TRIM(CAST(s.account_number AS VARCHAR))                                          AS account_id,
        CAST(s.balance            AS DECIMAL(18,2))                                      AS balance,
        COALESCE(b.municipal, TRIM(CAST(s.company AS VARCHAR)))                          AS municipal,
        CASE
            WHEN s.date_of_birth IS NULL THEN NULL
            ELSE TO_CHAR(CAST(s.date_of_birth AS DATE), 'YYYYMMDD')
        END                                                                              AS birth_date
    FROM hive.silver.t24_acct_cust s
    LEFT JOIN dim_branch b
        ON TRIM(CAST(s.company AS VARCHAR)) = b.co_code
    WHERE CAST(s.category AS INTEGER) = 6006
      AND CAST(s.balance AS DECIMAL(18,2)) > 0
),

silver_aged AS (
    SELECT
        account_id,
        balance,
        municipal,
        CASE
            WHEN birth_date IS NULL THEN NULL
            ELSE
                CASE
                    WHEN (
                        CAST(SUBSTR(birth_date, 5, 2) AS INTEGER) * 100
                        + CAST(SUBSTR(birth_date, 7, 2) AS INTEGER)
                    ) >
                    {% if td != '' %}
                    (
                        CAST(SUBSTR('{{ td }}', 6, 2) AS INTEGER) * 100
                        + CAST(SUBSTR('{{ td }}', 9, 2) AS INTEGER)
                    )
                    THEN CAST(SUBSTR('{{ td }}', 1, 4) AS INTEGER)
                       - CAST(SUBSTR(birth_date, 1, 4) AS INTEGER) - 1
                    ELSE CAST(SUBSTR('{{ td }}', 1, 4) AS INTEGER)
                       - CAST(SUBSTR(birth_date, 1, 4) AS INTEGER)
                    {% else %}
                    (
                        CAST(SUBSTR(CAST(CAST(CURRENT_DATE AS VARCHAR) AS VARCHAR), 6, 2) AS INTEGER) * 100
                        + CAST(SUBSTR(CAST(CAST(CURRENT_DATE AS VARCHAR) AS VARCHAR), 9, 2) AS INTEGER)
                    )
                    THEN EXTRACT(YEAR FROM CURRENT_DATE)
                       - CAST(SUBSTR(birth_date, 1, 4) AS INTEGER) - 1
                    ELSE EXTRACT(YEAR FROM CURRENT_DATE)
                       - CAST(SUBSTR(birth_date, 1, 4) AS INTEGER)
                    {% endif %}
                END
        END AS age
    FROM silver_src
),

silver AS (
    SELECT
        account_id,
        balance,
        municipal,
        age,
        CASE
            WHEN age IS NULL  THEN 'Unknown'
            WHEN age <= 5     THEN '0-5'
            WHEN age <= 10    THEN '6-10'
            WHEN age <= 14    THEN '11-14'
            ELSE '>15'
        END AS age_group
    FROM silver_aged
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

base AS (
    SELECT municipal, age_group, balance
    FROM silver
    WHERE age_group != 'Unknown'
),

pivoted AS (
    SELECT
        municipal,
        CAST(COUNT(CASE WHEN age_group = '0-5'   THEN account_id END) AS INTEGER)         AS age_0_5_count,
        CAST(SUM(CASE  WHEN age_group = '0-5'   THEN balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS age_0_5_amount,
        CAST(COUNT(CASE WHEN age_group = '6-10'  THEN account_id END) AS INTEGER)         AS age_6_10_count,
        CAST(SUM(CASE  WHEN age_group = '6-10'  THEN balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS age_6_10_amount,
        CAST(COUNT(CASE WHEN age_group = '11-14' THEN account_id END) AS INTEGER)         AS age_11_14_count,
        CAST(SUM(CASE  WHEN age_group = '11-14' THEN balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS age_11_14_amount,
        CAST(COUNT(CASE WHEN age_group = '>15'   THEN account_id END) AS INTEGER)         AS age_over15_count,
        CAST(SUM(CASE  WHEN age_group = '>15'   THEN balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS age_over15_amount
    FROM silver
    WHERE age_group != 'Unknown'
    GROUP BY municipal
),

municipal_rows AS (
    SELECT
        CAST(TO_DATE('{{ load_dt }}', 'YYYY-MM-DD') AS VARCHAR)               AS load_date,
        m.sort_order,
        m.municipal,
        CAST(COALESCE(p.age_0_5_count,    0) AS INTEGER)                    AS age_0_5_count,
        CAST(COALESCE(p.age_0_5_amount,   CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS age_0_5_amount,
        CAST(COALESCE(p.age_6_10_count,   0) AS INTEGER)                    AS age_6_10_count,
        CAST(COALESCE(p.age_6_10_amount,  CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS age_6_10_amount,
        CAST(COALESCE(p.age_11_14_count,  0) AS INTEGER)                    AS age_11_14_count,
        CAST(COALESCE(p.age_11_14_amount, CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS age_11_14_amount,
        CAST(COALESCE(p.age_over15_count,  0) AS INTEGER)                   AS age_over15_count,
        CAST(COALESCE(p.age_over15_amount, CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS age_over15_amount,
        CAST(0 AS INTEGER)                                                   AS inactive_count,
        CAST(0 AS DECIMAL(18,2))                                            AS inactive_amount,
        CAST(0 AS INTEGER)                                                   AS closed_count,
        CAST(0 AS DECIMAL(18,2))                                            AS closed_amount
    FROM municipal_labels m
    LEFT JOIN pivoted p ON m.municipal = p.municipal
),

total_row AS (
    SELECT
        CAST(TO_DATE('{{ load_dt }}', 'YYYY-MM-DD') AS VARCHAR)               AS load_date,
        99                                                                    AS sort_order,
        'TOTAL'                                                               AS municipal,
        CAST(SUM(age_0_5_count)     AS INTEGER)                              AS age_0_5_count,
        CAST(SUM(age_0_5_amount)    AS DECIMAL(18,2))                       AS age_0_5_amount,
        CAST(SUM(age_6_10_count)    AS INTEGER)                              AS age_6_10_count,
        CAST(SUM(age_6_10_amount)   AS DECIMAL(18,2))                       AS age_6_10_amount,
        CAST(SUM(age_11_14_count)   AS INTEGER)                              AS age_11_14_count,
        CAST(SUM(age_11_14_amount)  AS DECIMAL(18,2))                       AS age_11_14_amount,
        CAST(SUM(age_over15_count)  AS INTEGER)                              AS age_over15_count,
        CAST(SUM(age_over15_amount) AS DECIMAL(18,2))                       AS age_over15_amount,
        CAST(0 AS INTEGER)                                                   AS inactive_count,
        CAST(0 AS DECIMAL(18,2))                                            AS inactive_amount,
        CAST(0 AS INTEGER)                                                   AS closed_count,
        CAST(0 AS DECIMAL(18,2))                                            AS closed_amount
    FROM municipal_rows
),

combined AS (
    SELECT load_date, sort_order, municipal,
        age_0_5_count, age_0_5_amount,
        age_6_10_count, age_6_10_amount,
        age_11_14_count, age_11_14_amount,
        age_over15_count, age_over15_amount,
        inactive_count, inactive_amount,
        closed_count, closed_amount
    FROM municipal_rows
    UNION ALL
    SELECT load_date, sort_order, municipal,
        age_0_5_count, age_0_5_amount,
        age_6_10_count, age_6_10_amount,
        age_11_14_count, age_11_14_amount,
        age_over15_count, age_over15_amount,
        inactive_count, inactive_amount,
        closed_count, closed_amount
    FROM total_row
)

SELECT
    load_date,
    sort_order,
    municipal,
    age_0_5_count,
    age_0_5_amount,
    age_6_10_count,
    age_6_10_amount,
    age_11_14_count,
    age_11_14_amount,
    age_over15_count,
    age_over15_amount,
    inactive_count,
    inactive_amount,
    closed_count,
    closed_amount,
    CURRENT_TIMESTAMP AS updated_at
FROM combined

ORDER BY load_date, sort_order
