{{ config(
    materialized='table',
    unique_key=['load_date', 'branch_name', 'order_id'],
    incremental_strategy='merge',
    database='hive',
    schema='gold'
) }}

-- Division: accounting-income
-- Grain: 1 row per (load_date, branch_name, order_id)
-- 71 template rows (includes headers, data rows, subtotals, totals) per branch per date.
-- Income data from trial_balance_detail_v2 WHERE statement_type = 'PL'

WITH

-- 1. Report layout from seed
layout AS (
    SELECT
        CAST(order_id AS INT)       AS order_id,
        CAST(code AS VARCHAR)       AS code,
        CAST(description AS VARCHAR) AS description,
        CAST(line AS VARCHAR)       AS line,
        CAST(stt_noline AS INT)     AS stt_noline
    FROM {{ ref('map_detail_income') }}
),

-- 2. Distinct (load_date, branch_name) pairs from PL source
dates_branches AS (
    SELECT DISTINCT
        load_date,
        CAST(branch_name AS VARCHAR) AS branch_name
    FROM hive.silver.trial_balance_detail
    WHERE statement_type = 'PL'  -- Profit & Loss (income)
    {% if var("target_date") != "" %}
      AND load_date = DATE '{{ var("target_date") }}'
    {% endif %}
),

-- 3. Cross-join layout with all date/branch combos
report_frame AS (
    SELECT
        db.load_date,
        db.branch_name,
        l.order_id,
        l.code,
        l.description,
        l.line,
        l.stt_noline
    FROM dates_branches db
    CROSS JOIN layout l
),

-- 4. Leaf line amounts from staging (only for rows where line is not null)
line_amounts AS (
    SELECT
        load_date,
        branch_name,
        account_code AS line,
        closing_balance AS amount
    FROM {{ ref('stg__map_total_income') }}
    {% if var("target_date") != "" %}
    WHERE load_date = DATE '{{ var("target_date") }}'
    {% endif %}
),

-- 5. Assemble amounts for each row
assembled AS (
    SELECT
        rf.load_date,
        rf.branch_name,
        rf.order_id,
        rf.code,
        rf.description,
        rf.line,
        rf.stt_noline,
        -- Leaf rows: lookup by line code
        CASE
            WHEN rf.line IS NOT NULL
            THEN COALESCE(la.amount, CAST(0 AS DECIMAL(18,2)))
            -- Subtotal/header rows: write NULL (template has Excel formulas)
            ELSE NULL
        END AS amount
    FROM report_frame rf
    LEFT JOIN line_amounts la
        ON  rf.load_date   = la.load_date
        AND rf.branch_name = la.branch_name
        AND rf.line        = la.line
)

SELECT
    load_date,
    branch_name,
    order_id,
    code,
    description,
    line,
    stt_noline,
    CASE
        WHEN amount IS NOT NULL THEN CAST(amount AS DECIMAL(18,2))
        ELSE NULL
    END AS amount,
    CONCAT(
        CAST(CAST(SUBSTR(CAST(load_date AS VARCHAR), 9, 2) AS INTEGER) AS VARCHAR),
        CASE
            WHEN CAST(SUBSTR(CAST(load_date AS VARCHAR), 9, 2) AS INTEGER) IN (11, 12, 13) THEN 'th'
            WHEN MOD(CAST(SUBSTR(CAST(load_date AS VARCHAR), 9, 2) AS INTEGER), 10) = 1 THEN 'st'
            WHEN MOD(CAST(SUBSTR(CAST(load_date AS VARCHAR), 9, 2) AS INTEGER), 10) = 2 THEN 'nd'
            WHEN MOD(CAST(SUBSTR(CAST(load_date AS VARCHAR), 9, 2) AS INTEGER), 10) = 3 THEN 'rd'
            ELSE 'th'
        END,
        ' ',
        CASE CAST(SUBSTR(CAST(load_date AS VARCHAR), 6, 2) AS INTEGER)
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
        SUBSTR(CAST(load_date AS VARCHAR), 1, 4)
    ) AS load_date_label,
    CURRENT_TIMESTAMP AS updated_at
FROM assembled

ORDER BY order_id ASC
