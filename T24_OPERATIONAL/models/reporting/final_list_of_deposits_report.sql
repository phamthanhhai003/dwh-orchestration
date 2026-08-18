{{
    config(
        materialized='incremental',
        unique_key=['process_date', 'company_branch', 'account_no'],
        incremental_strategy='merge',
        database='hive',
        schema='gold'
    )
}}

-- Division: list_of_deposits | Report type: transaction_listing
-- Template: docs/report-template/list_of_deposists.xlsx Sheet1
-- Columns: ACCOUNT, PRODCAT, PROD NAME, CID, NAME, ACC OPENED DATE, maturity date, DOB, AMOUNT, COMPANY/BRANCH

-- Adapter shaping from stg__list_of_deposits_t24 folded inline (silver direct-read)
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
        CAST(NULLIF('{{ var("target_date", "") }}', '') AS DATE)                                     AS process_date,
        CAST(s.company AS VARCHAR)                                                                   AS company_branch,
        COALESCE(b.branch_name, TRIM(CAST(s.company AS VARCHAR)))                                   AS branch_name,
        CAST(s.account_number AS VARCHAR)                                                            AS account_no,
        CAST(s.category AS VARCHAR)                                                                  AS prodcat,
        CASE CAST(s.category AS VARCHAR)
            WHEN '1001' THEN 'Giro'
            WHEN '1002' THEN 'Government'
            WHEN '1003' THEN 'Group'
            WHEN '1190' THEN 'Time Deposite'
            WHEN '6005' THEN 'Passbook saving'
            WHEN '6006' THEN 'Futuru Savings'
            WHEN '6007' THEN 'Eldery Saving'
            WHEN '6008' THEN 'Pledge Saving'
            WHEN '6009' THEN 'Pledge Saving'
            WHEN '6010' THEN 'BDM JF'
            WHEN '6011' THEN 'BDM'
            WHEN '6012' THEN 'Asu''wain'
            WHEN '6013' THEN 'Pensionista'
            ELSE 'Unknown'
        END                                                                                          AS prod_name,
        CAST(s.customer_no AS VARCHAR)                                                               AS customer_id,
        CAST(s.short_name AS VARCHAR)                                                                AS cust_name,
        CAST(s.opening_date AS VARCHAR)                                                              AS opening_date,
        CAST(NULL AS VARCHAR)                                                                        AS maturity_date,
        CAST(s.date_of_birth AS VARCHAR)                                                             AS dob,
        CAST(s.balance AS DECIMAL(18,2))                                                             AS acct_bal
    FROM hive.silver.t24_acct_cust s
    LEFT JOIN dim_branch b
        ON TRIM(CAST(s.company AS VARCHAR)) = b.co_code
    WHERE CAST(s.category AS VARCHAR) IN (
          '1001','1002','1003','1190',
          '6005','6006','6007','6008','6009',
          '6010','6011','6012','6013'
      )
),

transactions AS (
    SELECT
        process_date,
        company_branch,
        ROW_NUMBER() OVER (
            PARTITION BY process_date, company_branch
            ORDER BY account_no
        )                                                                   AS row_no,
        account_no,
        prodcat,
        prod_name,
        customer_id,
        cust_name,
        opening_date,
        maturity_date,
        dob,
        CAST(acct_bal AS DECIMAL(18,2))                                     AS acct_bal,
        'DATA'                                                              AS row_type
    FROM silver
    WHERE 1=1
    {% if var("target_date", "") != "" %}
      AND process_date = CAST('{{ var("target_date") }}' AS DATE)
    {% endif %}
),

totals AS (
    SELECT
        process_date,
        company_branch,
        NULL                                                                AS row_no,
        'TOTAL'                                                             AS account_no,
        NULL                                                                AS prodcat,
        NULL                                                                AS prod_name,
        NULL                                                                AS customer_id,
        NULL                                                                AS cust_name,
        NULL                                                                AS opening_date,
        NULL                                                                AS maturity_date,
        NULL                                                                AS dob,
        CAST(SUM(acct_bal) AS DECIMAL(18,2))                               AS acct_bal,
        'TOTAL'                                                             AS row_type
    FROM transactions
    GROUP BY process_date, company_branch
)

SELECT
    process_date,
    company_branch,
    row_no,
    account_no,
    prodcat,
    prod_name,
    customer_id,
    cust_name,
    opening_date,
    maturity_date,
    dob,
    acct_bal,
    row_type,
    CURRENT_TIMESTAMP                                                       AS updated_at
FROM transactions

UNION ALL

SELECT
    process_date,
    company_branch,
    row_no,
    account_no,
    prodcat,
    prod_name,
    customer_id,
    cust_name,
    opening_date,
    maturity_date,
    dob,
    acct_bal,
    row_type,
    CURRENT_TIMESTAMP                                                       AS updated_at
FROM totals

ORDER BY company_branch, row_type DESC, row_no
