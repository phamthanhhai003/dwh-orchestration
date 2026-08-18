{{ config(materialized='table', schema='silver') }}

-- total_accounts cho ASSET report = số loan bên CRIS, CHỈ cho line LOAN (CURRENT_LOAN / PAST_DUE).
-- Khoá: (load_date, branch_name, line). Gold final_assets join theo line.
--
-- ⚠️ CHỈ giới hạn parent_line ∈ leaf loan lines (seed map_total_assets, parent_group
--    CURRENT_LOAN/PAST_DUE) — KHÔNG đếm mọi product_key trong detail (line aggregate
--    3390/9999 gom nhiều category → over-count khủng). + auto_class filter theo nhóm line:
--      CURRENT_LOAN → STANDARD / UNDER-SUPERVISION
--      PAST_DUE     → DOUBTFUL / SUB-STANDARD / LOSS
-- Branch không map được co_code (ATAURO/UAT_ENV/Head Office) → không join → không xuất (= 0 ở gold).

{% set run_date = var('business_date') %}

WITH loan_lines AS (
    SELECT CAST(account_code AS VARCHAR) AS line, parent_group
    FROM {{ ref('map_total_assets') }}
    WHERE parent_group IN ('CURRENT_LOAN', 'PAST_DUE')
),

-- (branch, line, product_key) distinct của detail loan rows, kèm nhóm line từ seed.
line_products AS (
    SELECT DISTINCT
        t.load_date,
        t.branch_name,
        t.co_code,
        CAST(t.parent_line AS VARCHAR) AS line,
        t.product_key,
        l.parent_group
    FROM hive.silver.trial_balance_detail t
    JOIN loan_lines l ON CAST(t.parent_line AS VARCHAR) = l.line
    WHERE t.statement_type = 'GL'
      AND t.original_line IS NULL
      AND t.product_key IS NOT NULL
      AND t.co_code IS NOT NULL
      AND t.load_date = DATE '{{ run_date }}'
),

cris AS (
    SELECT company_code, CAST(category AS VARCHAR) AS category, auto_class, loan_id
    FROM hive.silver.t24_cris_report
    WHERE business_date = DATE '{{ run_date }}'
),

matched AS (
    SELECT lp.load_date, lp.branch_name, lp.line, c.loan_id
    FROM line_products lp
    JOIN cris c
      ON c.company_code = lp.co_code
     AND c.category     = lp.product_key
     AND (
          (lp.parent_group = 'CURRENT_LOAN' AND c.auto_class IN ('STANDARD', 'UNDER-SUPERVISION'))
       OR (lp.parent_group = 'PAST_DUE'     AND c.auto_class IN ('DOUBTFUL', 'SUB-STANDARD', 'LOSS'))
     )
)

SELECT
    load_date,
    branch_name,
    line,
    COUNT(DISTINCT loan_id) AS total_accounts
FROM matched
GROUP BY load_date, branch_name, line
