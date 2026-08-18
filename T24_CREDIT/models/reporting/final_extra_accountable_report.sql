{{ config(
    materialized='incremental',
    unique_key=['loan_id', 'load_date', 'type_period'],
    incremental_strategy='merge',
    database='hive',
    schema='gold',
    tags=['soc', 'monthly', 'write_off_report']
) }}

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

-- Phase 5b direct-read: ref('stg__extra_accountable_t24') replaced with ref('t24_aa_wof_loans_report').
-- Adapter shaping folded inline: all column aliases from stg__extra_accountable_t24 reproduced here.
-- Gaps (honest NULLs, unchanged from adapter): current_interest, officer_name, telmobile
--   (WOF source carries no interest breakdown or officer/phone fields).
silver_wof AS (
    SELECT
        TRIM(CAST(account_id AS VARCHAR))       AS loan_id,
        TRIM(CAST(account_id AS VARCHAR))       AS original_loan_id,
        TRIM(CAST(cust_id AS VARCHAR))          AS customer_number,
        TRIM(CAST(cust_id AS VARCHAR))          AS customer_id,
        TRIM(CAST(customer_name AS VARCHAR))    AS customer_name,
        TRIM(CAST(acct_dao AS VARCHAR))         AS co_code,
        TRIM(CAST(product_desc AS VARCHAR))     AS product_name,
        wof_balance                             AS loan_outstanding,
        loan_amount                             AS base_amount,
        CAST(NULL AS VARCHAR)                   AS current_interest,  -- gap: no interest breakdown in WOF
        CAST(NULL AS VARCHAR)                   AS officer_name,      -- gap: no officer field in WOF
        CAST(NULL AS VARCHAR)                   AS telmobile,         -- gap: no phone field in WOF
        COALESCE(CAST(load_date AS DATE), CAST(NULLIF('{{ var("target_date", "") }}', '') AS DATE)) AS partition_date  -- WOF silver load_date, else target_date param; NULL when both unset (no CURRENT_DATE)
    FROM hive.silver.t24_aa_wof_loans_report
),

loan_master AS (
    SELECT * FROM silver_wof
),
customer AS (
    SELECT DISTINCT
        customer_id,
        officer_name,
        telmobile
    FROM silver_wof
),
final_report AS (
    SELECT
        -- Nu.: row number
        ROW_NUMBER() OVER (
            ORDER BY
                lm.loan_id
        ) AS nu,
        TRIM(REGEXP_REPLACE(lm.customer_name, '^(.*?)\s+(\S+(?:\s+\S+)*)\s+\2\s*$', '$1 $2')) AS name,
        -- CIF/CID: Customer Number
        lm.customer_number AS cif_cid,
        -- BRANCH: Co Code
        lm.co_code AS branch,
        COALESCE(b.branch_name, TRIM(CAST(lm.co_code AS VARCHAR))) AS branch_name,
        -- CREDIT TYPE: Product Name
        TRIM(
            COALESCE(
                NULLIF(CAST(lm.product_name AS VARCHAR), ''),
                ''
            )
        ) AS credit_type,
        -- LOAN ID: strip leading zeros
        CAST(CAST(lm.loan_id AS BIGINT) AS VARCHAR) AS loan_id,
        lm.loan_id AS original_loan_id,
        -- YEAR OF WRITE OFF: không có trong source → NULL
        CAST(NULL AS VARCHAR) AS year_of_write_off,
        -- PRINCIPAL: ABS(Loan Outstanding)
        CASE
            WHEN lm.loan_outstanding IS NULL
            OR TRIM(CAST(lm.loan_outstanding AS VARCHAR)) = '' THEN 0
            ELSE ABS(CAST(lm.loan_outstanding AS DOUBLE))
        END AS principal,
        -- Principal Interest: Current Interest
        CASE
            WHEN lm.current_interest IS NULL
            OR TRIM(CAST(lm.current_interest AS VARCHAR)) = '' THEN 0
            ELSE CAST(lm.current_interest AS DOUBLE)
        END AS principal_interest,
        -- Penalty Interest: không có trong source → NULL
        CAST(NULL AS DOUBLE) AS penalty_interest,
        -- BALANCE PRINCIPAL INTEREST + PENALTY INTEREST
        CASE
            WHEN lm.loan_outstanding IS NULL
            OR TRIM(CAST(lm.loan_outstanding AS VARCHAR)) = '' THEN 0
            ELSE ABS(CAST(lm.loan_outstanding AS DOUBLE))
        END + CASE
            WHEN lm.current_interest IS NULL
            OR TRIM(CAST(lm.current_interest AS VARCHAR)) = '' THEN 0
            ELSE CAST(lm.current_interest AS DOUBLE)
        END AS balance_principal_interest_penalty,
        -- TOTAL WRITE OFF: không có trong source → NULL
        CAST(NULL AS DOUBLE) AS total_write_off,
        -- Metadata
        CAST(lm.partition_date AS VARCHAR) AS load_date,
        'CURR' AS type_period,
        CONCAT(
            CAST(CAST(SUBSTR(CAST(lm.partition_date AS VARCHAR), 9, 2) AS INTEGER) AS VARCHAR),
            CASE
                WHEN CAST(SUBSTR(CAST(lm.partition_date AS VARCHAR), 9, 2) AS INTEGER) IN (11, 12, 13) THEN 'th'
                WHEN MOD(CAST(SUBSTR(CAST(lm.partition_date AS VARCHAR), 9, 2) AS INTEGER), 10) = 1 THEN 'st'
                WHEN MOD(CAST(SUBSTR(CAST(lm.partition_date AS VARCHAR), 9, 2) AS INTEGER), 10) = 2 THEN 'nd'
                WHEN MOD(CAST(SUBSTR(CAST(lm.partition_date AS VARCHAR), 9, 2) AS INTEGER), 10) = 3 THEN 'rd'
                ELSE 'th'
            END,
            ' ',
            CASE CAST(SUBSTR(CAST(lm.partition_date AS VARCHAR), 6, 2) AS INTEGER)
                WHEN 1  THEN 'January'
                WHEN 2  THEN 'February'
                WHEN 3  THEN 'March'
                WHEN 4  THEN 'April'
                WHEN 5  THEN 'May'
                WHEN 6  THEN 'June'
                WHEN 7  THEN 'July'
                WHEN 8  THEN 'August'
                WHEN 9  THEN 'September'
                WHEN 10 THEN 'October'
                WHEN 11 THEN 'November'
                WHEN 12 THEN 'December'
            END,
            ' ',
            SUBSTR(CAST(lm.partition_date AS VARCHAR), 1, 4)
        ) AS load_date_label,
        CURRENT_TIMESTAMP AS updated_at
    FROM
        loan_master lm
        LEFT JOIN customer c ON TRIM(lm.customer_number) = TRIM(c.customer_id)
        LEFT JOIN dim_branch b ON TRIM(CAST(lm.co_code AS VARCHAR)) = b.co_code
    WHERE
        lm.loan_id IS NOT NULL
        AND TRIM(lm.loan_id) <> ''
        AND lm.base_amount IS NOT NULL
        AND TRIM(lm.base_amount) NOT IN ('', '0')
        {% if var("target_date", "") != "" %}
        AND lm.partition_date = CAST('{{ var("target_date") }}' AS DATE)
        {% elif is_incremental() %}
        AND CAST(lm.partition_date AS VARCHAR) >= (
            SELECT COALESCE(MAX(load_date), '1900-01-01')
            FROM {{ this }}
            WHERE type_period = 'CURR'
        )
        {% endif %}
),

curr AS (
    SELECT * FROM final_report
)

{% if is_incremental() and var("target_date", "") != "" %}
,

prev_period AS (
    SELECT
        nu, name, cif_cid, branch, branch_name, credit_type, loan_id, original_loan_id,
        year_of_write_off, principal, principal_interest, penalty_interest,
        balance_principal_interest_penalty, total_write_off,
        CAST('{{ var("target_date") }}' AS VARCHAR) AS load_date,
        'PREV'                                     AS type_period,
        load_date_label,
        CURRENT_TIMESTAMP                          AS updated_at
    FROM {{ this }}
    WHERE type_period = 'CURR'
      AND load_date = (
          SELECT MAX(load_date) FROM {{ this }}
          WHERE type_period = 'CURR'
            AND CAST(load_date AS DATE) < CAST(DATE_TRUNC('month', CAST('{{ var("target_date") }}' AS DATE)) AS DATE)
      )
),

yoy_period AS (
    SELECT
        nu, name, cif_cid, branch, branch_name, credit_type, loan_id, original_loan_id,
        year_of_write_off, principal, principal_interest, penalty_interest,
        balance_principal_interest_penalty, total_write_off,
        CAST('{{ var("target_date") }}' AS VARCHAR) AS load_date,
        'YOY'                                      AS type_period,
        load_date_label,
        CURRENT_TIMESTAMP                          AS updated_at
    FROM {{ this }}
    WHERE type_period = 'CURR'
      AND load_date = (
          SELECT MAX(load_date) FROM {{ this }}
          WHERE type_period = 'CURR'
            AND CAST(DATE_TRUNC('year', CAST(load_date AS DATE)) AS DATE) = CAST(DATE_TRUNC('year', TIMESTAMPADD(YEAR, -1, CAST('{{ var("target_date") }}' AS DATE))) AS DATE)
            AND CAST(load_date AS DATE) <= CAST(TIMESTAMPADD(YEAR, -1, DATE_TRUNC('month', CAST('{{ var("target_date") }}' AS DATE))) AS DATE)
      )
)

SELECT * FROM curr
UNION ALL SELECT * FROM prev_period
UNION ALL SELECT * FROM yoy_period

{% else %}

SELECT * FROM curr

{% endif %}

ORDER BY principal DESC
