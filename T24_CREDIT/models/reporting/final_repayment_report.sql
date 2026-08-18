{{ config(
    materialized='incremental',
    unique_key=['loan_id', 'load_date', 'type_period'],
    incremental_strategy='merge',
    database='hive',
    schema='gold',
    tags=['soc', 'monthly', 'repayment_report', 'staging']
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

-- Phase 5b direct-read: ref('stg__repayment_t24') replaced with ref('t24_cris_report').
-- Adapter shaping folded inline: all column aliases from stg__repayment_t24 reproduced here.
-- Gaps (honest NULLs, unchanged from adapter): customer_name, commitment, term, partition_date,
--   cris_classification (absent-from-logic rule; auto_class covers COALESCE fallback).
silver_repayment AS (
    SELECT
        loan_id,
        category,
        product_name,
        customer_number,
        account_officer,
        currency,
        auto_class,
        loan_outstanding,
        provision_type,
        base_amount,
        commitment,                         -- NULL in silver (Workflow-A gap)
        term,                               -- NULL in silver (Workflow-A gap)
        maturity_date,
        repayment_amount,
        last_date_cr,
        opening_date,
        customer_name,                      -- NULL in silver (honest gap)
        company_code                    AS co_code,
        arrangement_id,
        legal_id,
        CAST(NULLIF('{{ var("target_date", "") }}', '') AS DATE) AS partition_date,  -- as-of date = target_date param only; NULL when unset (no CURRENT_DATE) -> full data, all rows
        CAST(NULL AS VARCHAR)           AS cris_classification, -- absent-from-logic
        loan_id                         AS account_number,
        customer_number                 AS customer_id,
        account_officer                 AS officer_name,
        mobile_phone                    AS telmobile
    FROM hive.silver.t24_cris_report
),

loan_master AS (
    SELECT * FROM silver_repayment
),
customer AS (
    SELECT DISTINCT
        customer_id,
        officer_name,
        telmobile
    FROM silver_repayment
),
provision AS (
    SELECT
        account_number,
        category,
        product_name,
        auto_class,
        loan_outstanding,
        provision_type,
        base_amount
    FROM silver_repayment
),
final_report AS (
    SELECT
        -- Loan Id: padded original
        lm.loan_id AS loan_id,
        lm.loan_id AS original_loan_id,
        lm.loan_id AS original_id,
        -- Loan Id2: strip leading zeros
        CAST(CAST(lm.loan_id AS BIGINT) AS VARCHAR) AS loan_id2,
        -- Category: prefer provision, fallback to loan_master
        TRIM(
            COALESCE(
                NULLIF(CAST(p.category AS VARCHAR), ''),
                NULLIF(CAST(lm.category AS VARCHAR), '')
            )
        ) AS category,
        -- Product Name: prefer provision, fallback to loan_master
        TRIM(
            COALESCE(
                NULLIF(CAST(p.product_name AS VARCHAR), ''),
                NULLIF(CAST(lm.product_name AS VARCHAR), '')
            )
        ) AS product_name,
        -- Customer Number
        lm.customer_number AS cust_number,
        -- Account Officer: prefer loan_master, fallback to customer
        COALESCE(
            NULLIF(TRIM(lm.account_officer), ''),
            c.officer_name
        ) AS account_officer,
        -- Currency
        lm.currency AS ccy,
        COALESCE(NULLIF(TRIM(lm.auto_class), ''), NULLIF(TRIM(lm.cris_classification), '')) AS classification,
        -- Outstanding: ABS, null-safe
        CASE
            WHEN lm.loan_outstanding IS NULL
            OR TRIM(CAST(lm.loan_outstanding AS VARCHAR)) = '' THEN 0
            ELSE ABS(CAST(lm.loan_outstanding AS DOUBLE))
        END AS outstanding,
        -- Provision Type: prefer provision, fallback to loan_master
        COALESCE(
            NULLIF(TRIM(p.provision_type), ''),
            NULLIF(TRIM(lm.provision_type), '')
        ) AS prov_type,
        -- Base Amount: ABS, null-safe
        CASE
            WHEN lm.base_amount IS NULL
            OR TRIM(CAST(lm.base_amount AS VARCHAR)) = '' THEN 0
            ELSE ABS(CAST(lm.base_amount AS DOUBLE))
        END AS base_amount,
        -- Commitment
        CASE
            WHEN lm.commitment IS NULL
            OR TRIM(CAST(lm.commitment AS VARCHAR)) = '' THEN 0
            ELSE CAST(lm.commitment AS DOUBLE)
        END AS commitment,
        -- Term
        lm.term AS term,
        -- Maturity Date
        lm.maturity_date AS maturity_date,
        -- Last Instalment (Repayment Amount from loan_master)
        CASE
            WHEN lm.repayment_amount IS NULL
            OR TRIM(CAST(lm.repayment_amount AS VARCHAR)) = '' THEN 0
            ELSE CAST(lm.repayment_amount AS DOUBLE)
        END AS last_instalment,
        -- Last Rep Date
        lm.last_date_cr AS last_rep_date,
        -- Start Date (Opening Date)
        lm.opening_date AS start_date,
        TRIM(REGEXP_REPLACE(lm.customer_name, '^(.*?)\s+(\S+(?:\s+\S+)*)\s+\2\s*$', '$1 $2')) AS customer_name,
        -- Branch Code (Co Code)
        lm.co_code AS branch_code,
        COALESCE(b.branch_name, TRIM(CAST(lm.co_code AS VARCHAR))) AS branch_name,
        -- Arrangement Id
        lm.arrangement_id AS arrangement_id,
        -- Legal ID
        lm.legal_id AS legal_id,
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
        LEFT JOIN provision p ON TRIM(lm.loan_id) = TRIM(p.account_number)
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
        loan_id, original_loan_id, original_id, loan_id2, category, product_name, cust_number,
        account_officer, ccy, classification, outstanding, prov_type, base_amount,
        commitment, term, maturity_date, last_instalment,
        last_rep_date, start_date, customer_name, branch_code, branch_name, arrangement_id, legal_id,
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
        loan_id, original_loan_id, original_id, loan_id2, category, product_name, cust_number,
        account_officer, ccy, classification, outstanding, prov_type, base_amount,
        commitment, term, maturity_date, last_instalment,
        last_rep_date, start_date, customer_name, branch_code, branch_name, arrangement_id, legal_id,
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

ORDER BY outstanding DESC
