{{ config(
    materialized='table',
    unique_key=['load_date', 'branch_name', 'account_code'],
    incremental_strategy='merge',
    database='hive',
    schema='silver'
) }}

-- Staging: map P&L account codes to parent groups
-- Reads from trial_balance_detail_v2 (PL = Profit & Loss = Income)
-- Outputs: one row per (load_date, branch_name, account_code) with closing_balance

WITH source_data AS (
    SELECT
        load_date,
        CAST(branch_name AS VARCHAR) AS branch_name,
        CAST(original_line AS VARCHAR) AS account_code,
        CAST(closing_balance AS DECIMAL(18,2)) AS closing_balance
    FROM hive.silver.trial_balance_detail
    WHERE statement_type = 'PL'  -- Profit & Loss (income)
      AND load_date = DATE '{{ var("target_date") }}'
),

base_income AS (
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
        CAST(be.closing_balance AS DECIMAL(18,2)) AS closing_balance
    FROM base_income be
    LEFT JOIN {{ ref('map_total_income') }} m
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

