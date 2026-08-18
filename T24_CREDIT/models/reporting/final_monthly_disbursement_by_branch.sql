{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['report_year', 'report_month', 'Branch', 'Credit_Product', 'type_period'],
    database = 'hive',
    schema = 'gold',
    tags = ['credit', 'monthly', 'disbursement_by_branch']
) }}

{% set report_year = var('report_year', '') %}
{# report_year passed as a run var; blank -> no year filter -> all years (full source). #}

-- Phase 5b direct-read: ref('stg_credit_disbursement_t24') replaced with direct silver refs.
-- Join logic from stg_credit_disbursement_t24 folded inline:
--   cris = t24_cris_report (opening_date, category, company_code, loan_id, arrangement_id)
--   commitment_from_al = t24_find_arrangement_al_bnctl (commitment_amt via arrangement join)
-- Output columns: report_year, report_month, co_code, category_code, total_disbursement, total_clients
WITH cris AS (
    SELECT
        loan_id,
        category,
        opening_date,
        company_code        AS co_code,
        arrangement_id
    FROM hive.silver.t24_cris_report
    WHERE opening_date IS NOT NULL
      AND category IS NOT NULL
      AND CAST(category AS VARCHAR) <> ''
      AND loan_id IS NOT NULL
      AND TRIM(loan_id) <> ''
),

commitment_from_al AS (
    SELECT
        TRIM(arrangement)   AS arr_id,
        commitment_amt
    FROM hive.silver.t24_find_arrangement_al_bnctl
    WHERE arrangement IS NOT NULL
      AND TRIM(arrangement) <> ''
),

loans AS (
    SELECT
        c.opening_date,
        c.co_code,
        c.category,
        c.loan_id,
        al.commitment_amt   AS commitment
    FROM cris c
    LEFT JOIN commitment_from_al al
        ON al.arr_id = TRIM(CAST(c.arrangement_id AS VARCHAR))
),

stg AS (
    SELECT
        EXTRACT(YEAR  FROM opening_date)    AS report_year,
        EXTRACT(MONTH FROM opening_date)    AS report_month,
        co_code                             AS co_code,
        CAST(category AS VARCHAR)           AS category_code,
        COALESCE(SUM(
            CASE
                WHEN commitment IS NULL THEN 0
                ELSE CAST(commitment AS DOUBLE)
            END
        ), 0)                               AS total_disbursement,
        COUNT(DISTINCT loan_id)             AS total_clients
    FROM loans
    WHERE opening_date IS NOT NULL
      AND loan_id IS NOT NULL
      AND TRIM(loan_id) <> ''
      {% if report_year != '' %}
      AND EXTRACT(YEAR FROM opening_date) = {{ report_year }}
      {% endif %}
    GROUP BY 1, 2, 3, 4
),

dim_branch AS (
    SELECT co_code, branch_name, branch_seq
    FROM (VALUES
        ('TL0010001', 'BNK',      1),
        ('TL0010002', 'DILI',     2),
        ('TL0010003', 'GLENU',    3),
        ('TL0010004', 'MALIANA',  4),
        ('TL0010005', 'AILEU',    5),
        ('TL0010006', 'OECUSSE',  6),
        ('TL0010007', 'BAUCAU',   7),
        ('TL0010008', 'SAME',     8),
        ('TL0010009', 'AINARO',   9),
        ('TL0010010', 'SUAI',     10),
        ('TL0010011', 'VIQUEQUE', 11),
        ('TL0010012', 'LOSPALOS', 12),
        ('TL0010013', 'LISQUICA', 13),
        ('TL0010014', 'MANATUTO', 14)
    ) AS t(co_code, branch_name, branch_seq)
),

dim_product AS (
    SELECT category_code, product_name, product_seq
    FROM (VALUES
        ('3001', 'Market Vendor Loan',  1),
        ('3002', 'Group Loan',          2),
        ('3003', 'Seasonal Loan',       3),
        ('3004', 'Business Loan',       4),
        ('3005', 'Transport Loan',      5),
        ('3006', 'Project Loan',        6),
        ('3007', 'Investmen Loan',      7),
        ('3008', 'FIAR',                8),
        ('3009', 'Agriculture Loan',    9),
        ('3021', 'Multi Purpose Loan',  10),
        ('3024', 'Buka''e',             11),
        ('3025', 'Asuwa''in',           12),
        ('3026', 'Pension Loan',        13),
        ('3027', 'Internal Staff Loan', 14),
        ('3028', 'RBL',                 15)
    ) AS t(category_code, product_name, product_seq)
),

dim_month AS (
    SELECT month_num
    FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12)) AS t(month_num)
),

report_years AS (
    SELECT DISTINCT report_year FROM stg
),

all_combinations AS (
    SELECT
        y.report_year,
        b.co_code,
        b.branch_seq,
        b.branch_name,
        p.category_code,
        p.product_seq,
        p.product_name,
        m.month_num
    FROM report_years y
    CROSS JOIN dim_branch b
    CROSS JOIN dim_product p
    CROSS JOIN dim_month m
),

product_rows AS (
    SELECT
        a.report_year,
        a.branch_seq,
        a.branch_name                               AS Branch,
        a.month_num                                 AS report_month,
        a.product_seq                               AS Credit_Product_No,
        a.product_name                              AS Credit_Product,
        CAST(NULL AS DOUBLE)                        AS monthly_target,
        COALESCE(s.total_disbursement, 0)           AS total_disbursement,
        COALESCE(s.total_clients, 0)                AS total_clients,
        CAST(NULL AS DOUBLE)                        AS variance,
        CAST(NULL AS DOUBLE)                        AS pct,
        0                                           AS is_total_row
    FROM
        all_combinations a
        LEFT JOIN stg s
            ON  a.report_year   = s.report_year
            AND a.co_code       = s.co_code
            AND a.category_code = s.category_code
            AND a.month_num     = s.report_month
),

branch_totals AS (
    SELECT
        report_year,
        branch_seq,
        Branch,
        report_month,
        CAST(NULL AS INTEGER)                               AS Credit_Product_No,
        'TOTAL'                                             AS Credit_Product,
        SUM(monthly_target)                                 AS monthly_target,
        SUM(total_disbursement)                             AS total_disbursement,
        SUM(total_clients)                                  AS total_clients,
        SUM(variance)                                       AS variance,
        SUM(total_disbursement) / NULLIF(SUM(monthly_target), 0) AS pct,
        1                                                   AS is_total_row
    FROM product_rows
    GROUP BY report_year, branch_seq, Branch, report_month
),

curr AS (
    SELECT
        report_year,
        branch_seq,
        report_month,
        Branch,
        Credit_Product_No,
        Credit_Product,
        is_total_row,
        monthly_target,
        total_disbursement,
        total_clients,
        variance,
        pct,
        'CURR' AS type_period,
        CONCAT(
            CASE report_month
                WHEN 1  THEN 'January'   WHEN 2  THEN 'February'  WHEN 3  THEN 'March'
                WHEN 4  THEN 'April'     WHEN 5  THEN 'May'        WHEN 6  THEN 'June'
                WHEN 7  THEN 'July'      WHEN 8  THEN 'August'     WHEN 9  THEN 'September'
                WHEN 10 THEN 'October'   WHEN 11 THEN 'November'   WHEN 12 THEN 'December'
            END,
            ' ',
            CAST(report_year AS VARCHAR)
        ) AS report_period_label,
        CURRENT_TIMESTAMP AS updated_at
    FROM (
        SELECT * FROM product_rows
        UNION ALL
        SELECT * FROM branch_totals
    )
)

{% if is_incremental() and report_year != '' %}
,

prev_period AS (
    SELECT
        {{ report_year }} AS report_year,
        COALESCE(d.branch_seq, 0) AS branch_seq,
        t.report_month,
        t.Branch,
        t.Credit_Product_No,
        t.Credit_Product,
        CASE WHEN t.Credit_Product = 'TOTAL' THEN 1 ELSE 0 END AS is_total_row,
        t.monthly_target,
        t.total_disbursement,
        t.total_clients,
        t.variance,
        t.pct,
        'PREV' AS type_period,
        t.report_period_label,
        CURRENT_TIMESTAMP AS updated_at
    FROM {{ this }} t
    LEFT JOIN dim_branch d ON t.Branch = d.branch_name
    WHERE t.type_period = 'CURR'
      AND t.report_year = (
          SELECT MAX(report_year) FROM {{ this }}
          WHERE type_period = 'CURR'
            AND report_year < {{ report_year }}
      )
),

yoy_period AS (
    SELECT
        {{ report_year }} AS report_year,
        COALESCE(d.branch_seq, 0) AS branch_seq,
        t.report_month,
        t.Branch,
        t.Credit_Product_No,
        t.Credit_Product,
        CASE WHEN t.Credit_Product = 'TOTAL' THEN 1 ELSE 0 END AS is_total_row,
        t.monthly_target,
        t.total_disbursement,
        t.total_clients,
        t.variance,
        t.pct,
        'YOY' AS type_period,
        t.report_period_label,
        CURRENT_TIMESTAMP AS updated_at
    FROM {{ this }} t
    LEFT JOIN dim_branch d ON t.Branch = d.branch_name
    WHERE t.type_period = 'CURR'
      AND t.report_year = (
          SELECT MAX(report_year) FROM {{ this }}
          WHERE type_period = 'CURR'
            AND report_year < (
                SELECT MAX(report_year) FROM {{ this }}
                WHERE type_period = 'CURR'
                  AND report_year < {{ report_year }}
            )
      )
)

SELECT * FROM curr
UNION ALL SELECT * FROM prev_period
UNION ALL SELECT * FROM yoy_period

{% else %}

SELECT * FROM curr

{% endif %}

ORDER BY
    report_month,
    branch_seq,
    is_total_row,
    Credit_Product_No
