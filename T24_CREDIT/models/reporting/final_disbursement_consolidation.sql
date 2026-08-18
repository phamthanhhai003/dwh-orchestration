{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['report_year', 'Branch', 'Credit_Product', 'type_period'],
    database = 'hive',
    schema = 'gold',
    tags = ['credit', 'yearly', 'disbursement_consolidation']
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

-- Years present in the filtered source (1 year when report_year passed; all years when blank).
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
        p.product_name
    FROM report_years y
    CROSS JOIN dim_branch b
    CROSS JOIN dim_product p
),

pivoted AS (
    SELECT
        a.report_year,
        a.branch_seq,
        a.branch_name,
        a.product_seq,
        a.product_name,
        SUM(CASE WHEN s.report_month = 1  THEN s.total_disbursement ELSE 0 END) AS amount_jan,
        SUM(CASE WHEN s.report_month = 2  THEN s.total_disbursement ELSE 0 END) AS amount_feb,
        SUM(CASE WHEN s.report_month = 3  THEN s.total_disbursement ELSE 0 END) AS amount_mar,
        SUM(CASE WHEN s.report_month = 4  THEN s.total_disbursement ELSE 0 END) AS amount_apr,
        SUM(CASE WHEN s.report_month = 5  THEN s.total_disbursement ELSE 0 END) AS amount_may,
        SUM(CASE WHEN s.report_month = 6  THEN s.total_disbursement ELSE 0 END) AS amount_jun,
        SUM(CASE WHEN s.report_month = 7  THEN s.total_disbursement ELSE 0 END) AS amount_jul,
        SUM(CASE WHEN s.report_month = 8  THEN s.total_disbursement ELSE 0 END) AS amount_aug,
        SUM(CASE WHEN s.report_month = 9  THEN s.total_disbursement ELSE 0 END) AS amount_sep,
        SUM(CASE WHEN s.report_month = 10 THEN s.total_disbursement ELSE 0 END) AS amount_oct,
        SUM(CASE WHEN s.report_month = 11 THEN s.total_disbursement ELSE 0 END) AS amount_nov,
        SUM(CASE WHEN s.report_month = 12 THEN s.total_disbursement ELSE 0 END) AS amount_dec,
        SUM(CASE WHEN s.report_month = 1  THEN s.total_clients ELSE 0 END) AS acct_jan,
        SUM(CASE WHEN s.report_month = 2  THEN s.total_clients ELSE 0 END) AS acct_feb,
        SUM(CASE WHEN s.report_month = 3  THEN s.total_clients ELSE 0 END) AS acct_mar,
        SUM(CASE WHEN s.report_month = 4  THEN s.total_clients ELSE 0 END) AS acct_apr,
        SUM(CASE WHEN s.report_month = 5  THEN s.total_clients ELSE 0 END) AS acct_may,
        SUM(CASE WHEN s.report_month = 6  THEN s.total_clients ELSE 0 END) AS acct_jun,
        SUM(CASE WHEN s.report_month = 7  THEN s.total_clients ELSE 0 END) AS acct_jul,
        SUM(CASE WHEN s.report_month = 8  THEN s.total_clients ELSE 0 END) AS acct_aug,
        SUM(CASE WHEN s.report_month = 9  THEN s.total_clients ELSE 0 END) AS acct_sep,
        SUM(CASE WHEN s.report_month = 10 THEN s.total_clients ELSE 0 END) AS acct_oct,
        SUM(CASE WHEN s.report_month = 11 THEN s.total_clients ELSE 0 END) AS acct_nov,
        SUM(CASE WHEN s.report_month = 12 THEN s.total_clients ELSE 0 END) AS acct_dec,
        COALESCE(SUM(s.total_disbursement), 0) AS amount_total,
        COALESCE(SUM(s.total_clients), 0)      AS account_total
    FROM
        all_combinations a
        LEFT JOIN stg s
            ON  a.report_year   = s.report_year
            AND a.co_code       = s.co_code
            AND a.category_code = s.category_code
    GROUP BY
        a.report_year, a.branch_seq, a.branch_name, a.product_seq, a.product_name
),

product_rows AS (
    SELECT
        report_year,
        branch_seq,
        branch_name AS Branch,
        product_seq AS Credit_Product_No,
        product_name AS Credit_Product,
        amount_jan, amount_feb, amount_mar, amount_apr, amount_may, amount_jun,
        amount_jul, amount_aug, amount_sep, amount_oct, amount_nov, amount_dec,
        acct_jan,   acct_feb,   acct_mar,   acct_apr,   acct_may,   acct_jun,
        acct_jul,   acct_aug,   acct_sep,   acct_oct,   acct_nov,   acct_dec,
        amount_total,
        account_total,
        0 AS is_total_row
    FROM pivoted
),

branch_totals AS (
    SELECT
        report_year,
        branch_seq,
        Branch,
        CAST(NULL AS INTEGER) AS Credit_Product_No,
        'TOTAL' AS Credit_Product,
        SUM(amount_jan), SUM(amount_feb), SUM(amount_mar), SUM(amount_apr),
        SUM(amount_may), SUM(amount_jun), SUM(amount_jul), SUM(amount_aug),
        SUM(amount_sep), SUM(amount_oct), SUM(amount_nov), SUM(amount_dec),
        SUM(acct_jan),   SUM(acct_feb),   SUM(acct_mar),   SUM(acct_apr),
        SUM(acct_may),   SUM(acct_jun),   SUM(acct_jul),   SUM(acct_aug),
        SUM(acct_sep),   SUM(acct_oct),   SUM(acct_nov),   SUM(acct_dec),
        SUM(amount_total)  AS amount_total,
        SUM(account_total) AS account_total,
        1 AS is_total_row
    FROM product_rows
    GROUP BY report_year, branch_seq, Branch
),

curr AS (
    SELECT
        report_year,
        branch_seq,
        Branch,
        Credit_Product_No,
        Credit_Product,
        is_total_row,
        amount_jan,  acct_jan,
        amount_feb,  acct_feb,
        amount_mar,  acct_mar,
        amount_apr,  acct_apr,
        amount_may,  acct_may,
        amount_jun,  acct_jun,
        amount_jul,  acct_jul,
        amount_aug,  acct_aug,
        amount_sep,  acct_sep,
        amount_oct,  acct_oct,
        amount_nov,  acct_nov,
        amount_dec,  acct_dec,
        amount_total,
        account_total,
        'CURR' AS type_period,
        CAST(report_year AS VARCHAR) AS report_year_label,
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
        t.Branch,
        t.Credit_Product_No,
        t.Credit_Product,
        CASE WHEN t.Credit_Product = 'TOTAL' THEN 1 ELSE 0 END AS is_total_row,
        t.amount_jan,  t.acct_jan,
        t.amount_feb,  t.acct_feb,
        t.amount_mar,  t.acct_mar,
        t.amount_apr,  t.acct_apr,
        t.amount_may,  t.acct_may,
        t.amount_jun,  t.acct_jun,
        t.amount_jul,  t.acct_jul,
        t.amount_aug,  t.acct_aug,
        t.amount_sep,  t.acct_sep,
        t.amount_oct,  t.acct_oct,
        t.amount_nov,  t.acct_nov,
        t.amount_dec,  t.acct_dec,
        t.amount_total,
        t.account_total,
        'PREV' AS type_period,
        CAST(t.report_year AS VARCHAR) AS report_year_label,
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
        t.Branch,
        t.Credit_Product_No,
        t.Credit_Product,
        CASE WHEN t.Credit_Product = 'TOTAL' THEN 1 ELSE 0 END AS is_total_row,
        t.amount_jan,  t.acct_jan,
        t.amount_feb,  t.acct_feb,
        t.amount_mar,  t.acct_mar,
        t.amount_apr,  t.acct_apr,
        t.amount_may,  t.acct_may,
        t.amount_jun,  t.acct_jun,
        t.amount_jul,  t.acct_jul,
        t.amount_aug,  t.acct_aug,
        t.amount_sep,  t.acct_sep,
        t.amount_oct,  t.acct_oct,
        t.amount_nov,  t.acct_nov,
        t.amount_dec,  t.acct_dec,
        t.amount_total,
        t.account_total,
        'YOY' AS type_period,
        CAST(t.report_year AS VARCHAR) AS report_year_label,
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

ORDER BY branch_seq, is_total_row, Credit_Product_No
