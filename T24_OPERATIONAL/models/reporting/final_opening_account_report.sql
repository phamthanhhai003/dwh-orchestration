{{
    config(
        materialized='incremental',
        unique_key=['opening_date', 'company_branch', 'account_no'],
        incremental_strategy='merge',
        database='hive',
        schema='gold'
    )
}}

-- Division: opening_account | Report type: transaction_listing
-- Template: docs/report-template/opening_and_closed.xlsx Sheet "Account Opening"
-- Columns: Branch.No, Branch.Name, Acct.No, Cus.No, Cus.Name, Open.Acct.Date,
--          Ident.Doc.Type, Ident.Doc.No, Alter ID Type, Alter ID No, PEP,
--          DOB, Nationality, Birth.Country, Address, Occupation, Acct.Bal, First.Deposit.Amt
-- Note: opening_date (DATE YYYY-MM-DD) is the primary date dimension; process_date is ETL snapshot date

-- Adapter shaping from stg__opening_account_t24 folded inline (silver direct-read)
WITH silver AS (
    SELECT
        CAST(CAST(s.opening_date AS DATE) AS VARCHAR)                                                  AS opening_date,
        TRIM(CAST(s.company AS VARCHAR))                                                               AS company_branch,
        TRIM(CAST(s.account_number AS VARCHAR))                                                        AS account_no,
        CAST(NULL AS VARCHAR)                                                                          AS branch_name,
        TRIM(CAST(s.customer_no AS VARCHAR))                                                           AS customer_id,
        LTRIM(RTRIM(CONCAT(
            COALESCE(TRIM(CAST(s.name1 AS VARCHAR)), ''), ' ',
            COALESCE(TRIM(CAST(s.name2 AS VARCHAR)), ''), ' ',
            COALESCE(TRIM(CAST(s.short_name AS VARCHAR)), '')
        )))                                                                                            AS cust_name,
        TRIM(CAST(s.legal_doc_name AS VARCHAR))                                                        AS id_doc_type,
        TRIM(CAST(s.legal_id AS VARCHAR))                                                              AS id_doc_no,
        CAST(NULL AS VARCHAR)                                                                          AS alter_id_type,
        CAST(NULL AS VARCHAR)                                                                          AS alter_id_no,
        CAST(NULL AS VARCHAR)                                                                          AS pep,
        CAST(CAST(s.date_of_birth AS DATE) AS VARCHAR)                                                 AS dob,
        TRIM(CAST(s.nationality AS VARCHAR))                                                           AS nationality,
        TRIM(CAST(s.birth_country AS VARCHAR))                                                         AS birth_country,
        CAST(s.address AS VARCHAR)                                                                     AS address,
        TRIM(CAST(s.occupation AS VARCHAR))                                                            AS occupation,
        CAST(s.balance AS DECIMAL(18,2))                                                               AS acct_bal,
        CAST(0 AS DECIMAL(18,2))                                                                       AS first_deposit_amt
    FROM hive.silver.t24_acct_cust s
    WHERE s.opening_date IS NOT NULL
    {% if var("target_date", "") != "" %}
      AND CAST(s.opening_date AS DATE) = CAST('{{ var("target_date") }}' AS DATE)
    {% endif %}
),

transactions AS (
    SELECT
        opening_date,
        company_branch,
        ROW_NUMBER() OVER (
            PARTITION BY opening_date, company_branch
            ORDER BY account_no
        )                                                                   AS row_no,
        account_no,
        company_branch                                                      AS branch_no,
        branch_name,
        customer_id,
        cust_name,
        id_doc_type,
        id_doc_no,
        alter_id_type,
        alter_id_no,
        pep,
        dob,
        nationality,
        birth_country,
        address,
        occupation,
        CAST(acct_bal AS DECIMAL(18,2))                                     AS acct_bal,
        CAST(first_deposit_amt AS DECIMAL(18,2))                            AS first_deposit_amt,
        'DATA'                                                              AS row_type
    FROM silver
    WHERE 1=1
    {% if var("target_date", "") != "" %}
      AND opening_date = '{{ var("target_date") }}'
    {% endif %}
),

totals AS (
    SELECT
        opening_date,
        company_branch,
        NULL                                                                AS row_no,
        'TOTAL'                                                             AS account_no,
        NULL                                                                AS branch_no,
        NULL                                                                AS branch_name,
        NULL                                                                AS customer_id,
        NULL                                                                AS cust_name,
        NULL                                                                AS id_doc_type,
        NULL                                                                AS id_doc_no,
        NULL                                                                AS alter_id_type,
        NULL                                                                AS alter_id_no,
        NULL                                                                AS pep,
        NULL                                                                AS dob,
        NULL                                                                AS nationality,
        NULL                                                                AS birth_country,
        NULL                                                                AS address,
        NULL                                                                AS occupation,
        CAST(SUM(acct_bal) AS DECIMAL(18,2))                               AS acct_bal,
        CAST(SUM(first_deposit_amt) AS DECIMAL(18,2))                      AS first_deposit_amt,
        'TOTAL'                                                             AS row_type
    FROM transactions
    GROUP BY opening_date, company_branch
)

SELECT
    opening_date,
    company_branch,
    row_no,
    account_no,
    branch_no,
    branch_name,
    customer_id,
    cust_name,
    id_doc_type,
    id_doc_no,
    alter_id_type,
    alter_id_no,
    pep,
    dob,
    nationality,
    birth_country,
    address,
    occupation,
    acct_bal,
    first_deposit_amt,
    row_type,
    CURRENT_TIMESTAMP                                                       AS updated_at
FROM transactions

UNION ALL

SELECT
    opening_date,
    company_branch,
    row_no,
    account_no,
    branch_no,
    branch_name,
    customer_id,
    cust_name,
    id_doc_type,
    id_doc_no,
    alter_id_type,
    alter_id_no,
    pep,
    dob,
    nationality,
    birth_country,
    address,
    occupation,
    acct_bal,
    first_deposit_amt,
    row_type,
    CURRENT_TIMESTAMP                                                       AS updated_at
FROM totals

ORDER BY company_branch, row_type DESC, row_no
