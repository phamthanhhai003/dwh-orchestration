{{ config(
    materialized='incremental',
    unique_key=['loan_id', 'load_date', 'type_period'],
    incremental_strategy='merge',
    database='hive',
    schema='gold',
    tags=['soc', 'monthly', 'provision_mapping_report', 'staging']
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

-- Phase 5b direct-read: ref('stg__provision_t24') replaced with ref('t24_cris_report').
-- Adapter shaping folded inline: all column aliases from stg__provision_t24 reproduced here.
-- Gaps (honest NULLs, unchanged from adapter): partition_date, cris_classification.
-- customer_name now sourced from t24_cris_report (CUSTOMER.NAME.1); customer_short_name added (CUSTOMER.NAME.2).
silver_provision AS (
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
        commitment,
        maturity_date,
        arrangement_status,
        repayment_amount,
        last_date_cr,
        opening_date,
        id_type,
        customer_name,                                  -- CUSTOMER.NAME.1 (sourced in t24_cris_report)
        -- customer_short_name DROPPED 2026-06-23 (not in Provision template "5. Provision Report.xlsx"; client confirmed not needed)
        company_code                    AS co_code,
        arrangement_id,
        legal_id,
        employer_name,
        interest_rate,
        current_interest,
        occupation,
        mobile_phone                    AS customer_telephone_mobile,
        loan_id                         AS account_number,
        customer_number                 AS customer_id,
        account_officer                 AS officer_name,
        mobile_phone                    AS telmobile,
        CAST(NULL AS VARCHAR)           AS cris_classification,  -- Workflow-A gap
        -- as-of date = target_date param when given; else default to the data's latest activity date
        -- (max last_date_cr in cris) so full-refresh load_date is NOT NULL (RECONCILE 2026-06-23;
        -- was CAST(NULL) on blank target_date -> load_date fully NULL in full-refresh).
        COALESCE(
            CAST(NULLIF('{{ var("target_date", "") }}', '') AS DATE),
            asof.asof_date
        ) AS partition_date
    FROM hive.silver.t24_cris_report
    CROSS JOIN (SELECT MAX(CAST(last_date_cr AS DATE)) AS asof_date FROM hive.silver.t24_cris_report) asof
),

loan_master AS (
    SELECT * FROM silver_provision
),
customer AS (
    SELECT DISTINCT
        customer_id,
        officer_name,
        telmobile
    FROM silver_provision
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
    FROM silver_provision
),
final_report AS (
    SELECT
        -- Loan_ID: strip leading zeros
        CAST(CAST(lm.loan_id AS BIGINT) AS VARCHAR) AS loan_id,
        lm.loan_id AS original_loan_id,
        -- Arrangement_Id
        lm.arrangement_id AS arrangement_id,
        -- Branch Code
        lm.co_code AS branch_code,
        COALESCE(b.branch_name, TRIM(CAST(lm.co_code AS VARCHAR))) AS branch_name,
        -- Cust_Number
        lm.customer_number AS cust_number,
        TRIM(REGEXP_REPLACE(lm.customer_name, '^(.*?)\s+(\S+(?:\s+\S+)*)\s+\2\s*$', '$1 $2')) AS customer_name,
        -- customer_short_name column DROPPED 2026-06-23 (not in template; client confirmed not needed)
        -- Category: prefer provision, fallback loan_master
        TRIM(
            COALESCE(
                NULLIF(CAST(p.category AS VARCHAR), ''),
                NULLIF(CAST(lm.category AS VARCHAR), '')
            )
        ) AS category,
        -- Product_Name: prefer provision, fallback loan_master
        TRIM(
            COALESCE(
                NULLIF(CAST(p.product_name AS VARCHAR), ''),
                NULLIF(CAST(lm.product_name AS VARCHAR), '')
            )
        ) AS product_name,
        -- Currency
        lm.currency AS currency,
        COALESCE(
            NULLIF(TRIM(lm.auto_class), ''),
            NULLIF(TRIM(p.auto_class), ''),
            NULLIF(TRIM(lm.cris_classification), '')
        ) AS classification,
        -- Local_CCY_Amt: Loan Outstanding numeric
        CASE
            WHEN lm.loan_outstanding IS NULL
            OR TRIM(CAST(lm.loan_outstanding AS VARCHAR)) = '' THEN 0
            ELSE CAST(lm.loan_outstanding AS DOUBLE)
        END AS local_ccy_amt,
        -- Base_Amount: numeric
        CASE
            WHEN p.base_amount IS NULL
            OR TRIM(CAST(p.base_amount AS VARCHAR)) = '' THEN CASE
                WHEN lm.base_amount IS NULL
                OR TRIM(CAST(lm.base_amount AS VARCHAR)) = '' THEN 0
                ELSE CAST(lm.base_amount AS DOUBLE)
            END
            ELSE CAST(p.base_amount AS DOUBLE)
        END AS base_amount,
        -- Outstanding
        CASE
            WHEN lm.loan_outstanding IS NULL
            OR TRIM(CAST(lm.loan_outstanding AS VARCHAR)) = '' THEN 0
            ELSE CAST(lm.loan_outstanding AS DOUBLE)
        END AS outstanding,
        -- Value_Date: Opening Date
        lm.opening_date AS value_date,
        -- Commitment
        CASE
            WHEN lm.commitment IS NULL
            OR TRIM(CAST(lm.commitment AS VARCHAR)) = '' THEN 0
            ELSE CAST(lm.commitment AS DOUBLE)
        END AS commitment,
        -- Start_Date: Opening Date
        lm.opening_date AS start_date,
        -- Mat_Date: Maturity Date
        lm.maturity_date AS mat_date,
        -- Provision
        CASE
            WHEN lm.base_amount IS NULL
            OR TRIM(CAST(lm.base_amount AS VARCHAR)) = '' THEN 0
            WHEN COALESCE(NULLIF(TRIM(lm.auto_class),''), NULLIF(TRIM(p.auto_class),''), NULLIF(TRIM(lm.cris_classification),'')) = 'LOSS'         THEN ABS(CAST(lm.base_amount AS DOUBLE))
            WHEN COALESCE(NULLIF(TRIM(lm.auto_class),''), NULLIF(TRIM(p.auto_class),''), NULLIF(TRIM(lm.cris_classification),'')) = 'DOUBTFUL'     THEN ABS(CAST(lm.base_amount AS DOUBLE)) * 0.50
            WHEN COALESCE(NULLIF(TRIM(lm.auto_class),''), NULLIF(TRIM(p.auto_class),''), NULLIF(TRIM(lm.cris_classification),'')) = 'SUB-STANDARD' THEN ABS(CAST(lm.base_amount AS DOUBLE)) * 0.25
            ELSE 0
        END AS provision,
        -- Status: Arrangement Status
        lm.arrangement_status AS status,
        -- Phone Number
        COALESCE(
            NULLIF(TRIM(lm.customer_telephone_mobile), ''),
            NULLIF(TRIM(c.telmobile), '')
        ) AS phone_number,
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
        loan_id, original_loan_id, arrangement_id, branch_code, branch_name, cust_number, customer_name,
        category, product_name, currency, classification, local_ccy_amt, base_amount,
        outstanding, value_date, commitment, start_date, mat_date,
        provision, status, phone_number,
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
        loan_id, original_loan_id, arrangement_id, branch_code, branch_name, cust_number, customer_name,
        category, product_name, currency, classification, local_ccy_amt, base_amount,
        outstanding, value_date, commitment, start_date, mat_date,
        provision, status, phone_number,
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

-- outstanding is already ABS(loan_outstanding) — ABS(outstanding) caused Dremio to project
-- an extra EXPR column in MERGE mode; ORDER BY outstanding is equivalent and avoids the issue.
ORDER BY outstanding DESC
