{{ config(
    materialized='table',
    unique_key=['load_date', 'branch_name', 'account_code'],
    incremental_strategy='merge',
    database='hive',
    schema='silver'
) }}

-- Staging: map P&L account codes to parent groups
-- Reads directly from accounting_parsed.data (CRF_PL = Profit & Loss)
-- Applies Line 1890 HO exclusion (write 0 for Head Office)
-- Outputs: one row per (load_date, branch_name, account_code) with closing_balance

WITH source_data AS (
    SELECT
        load_date,
        CAST(branch_name AS VARCHAR) AS branch_name,
        CAST(original_line AS VARCHAR) AS account_code,
        CAST(closing_balance AS DECIMAL(18,2)) AS closing_balance
    FROM hive.silver.trial_balance_detail
    WHERE statement_type = 'PL'  -- Profit & Loss (expenses)
      AND load_date = DATE '{{ var("target_date") }}'
),

base_expenses AS (
    SELECT
        load_date,
        branch_name,
        account_code,
        SUM(closing_balance) AS closing_balance
    FROM source_data
    GROUP BY 1, 2, 3
),

-- Join with seed to get parent_group categorization
mapped AS (
    SELECT
        be.load_date,
        be.branch_name,
        be.account_code,
        m.account_name,
        m.parent_group,
        -- Line 1890 HO exclusion: write 0 for Head Office, else actual balance
        CASE
            WHEN be.branch_name = 'Head Office' AND be.account_code = '1890'
            THEN CAST(0 AS DECIMAL(18,2))
            ELSE CAST(be.closing_balance AS DECIMAL(18,2))
        END AS closing_balance
    FROM base_expenses be
    LEFT JOIN {{ ref('map_total_expense') }} m
        ON be.account_code = CAST(m.account_code AS VARCHAR)
)

SELECT
    load_date,
    branch_name,
    account_code,
    account_name,
    parent_group,
    closing_balance,
    CURRENT_TIMESTAMP AS updated_at
FROM mapped

