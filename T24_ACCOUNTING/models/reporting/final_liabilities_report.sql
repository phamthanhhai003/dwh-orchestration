{{ config(materialized='table', schema='gold') }}

-- LIABILITIES report (per branch). Migrate từ ACCOUNTING_REPORTS:
--   * amount        : trial_balance header rows (closing_balance) theo line (+ remap 53xx→5320).
--   * total_accounts: ĐỔI nguồn → acct_count_liabilities (CRB, deposit) qua bridge co_code→branch.
-- Difference order_id 82 = TotalLiab(stt13) - (line 7240 + line 7045).

{% set run_date = var('target_date') %}

WITH seed_map AS (
    SELECT CAST(order_id AS INT) AS order_id, code, description, line, CAST(stt_noline AS INT) AS stt_noline
    FROM {{ ref('map_detail_liabilities') }}
),

base_balances_mapped AS (
    SELECT
        load_date, branch_name,
        CASE WHEN CAST(original_line AS VARCHAR) IN
                ('5321','5327','5324','5325','5341','5342','6660','7160','5323','5343','5326','5328')
             THEN '5320' ELSE CAST(original_line AS VARCHAR) END AS mapped_line,
        SUM(closing_balance) AS amt
    FROM hive.silver.trial_balance_detail
    WHERE load_date = DATE '{{ run_date }}' AND statement_type = 'GL' AND original_line IS NOT NULL
    GROUP BY 1, 2, 3
),

branch_co AS (
    SELECT DISTINCT branch_name, co_code
    FROM hive.silver.trial_balance_detail
    WHERE co_code IS NOT NULL AND load_date = DATE '{{ run_date }}'
),

acc_mapped AS (   -- count CRB qua bridge co_code→branch (remap 53xx vô hại với line deposit 43xx/44xx)
    SELECT
        a.load_date, bc.branch_name,
        CASE WHEN a.line IN ('5321','5327','5324','5325','5341','5342','6660','7160','5323','5343','5326','5328')
             THEN '5320' ELSE a.line END AS mapped_line,
        SUM(a.total_accounts) AS acc
    FROM {{ ref('acct_count_liabilities') }} a
    JOIN branch_co bc ON a.co_code = bc.co_code
    WHERE a.load_date = DATE '{{ run_date }}'
    GROUP BY 1, 2, 3
),

summary_data AS (
    SELECT load_date, branch_name, CAST(stt AS INT) AS stt_key, amount AS total_amt, total_accounts AS total_acc
    FROM {{ ref('stg__map_total_liabilities') }}
),

branch_spine AS (SELECT DISTINCT load_date, branch_name FROM base_balances_mapped),

final_join AS (
    SELECT
        bs.load_date, bs.branch_name, s.order_id, s.code, s.description, s.line, s.stt_noline,
        CASE WHEN s.line IS NOT NULL THEN COALESCE(b.amt, 0) ELSE COALESCE(sd.total_amt, 0) END AS final_amount,
        CASE WHEN s.line IS NOT NULL THEN COALESCE(ac.acc, 0) ELSE COALESCE(sd.total_acc, 0) END AS final_accounts
    FROM seed_map s
    CROSS JOIN branch_spine bs
    LEFT JOIN base_balances_mapped b ON CAST(s.line AS VARCHAR) = CAST(b.mapped_line AS VARCHAR) AND bs.branch_name = b.branch_name AND bs.load_date = b.load_date
    LEFT JOIN acc_mapped ac ON CAST(s.line AS VARCHAR) = CAST(ac.mapped_line AS VARCHAR) AND bs.branch_name = ac.branch_name AND bs.load_date = ac.load_date
    LEFT JOIN summary_data sd ON s.stt_noline = sd.stt_key AND bs.branch_name = sd.branch_name AND bs.load_date = sd.load_date
),

calculated_difference AS (
    SELECT *,
        MAX(CASE WHEN stt_noline = 13 THEN final_amount ELSE 0 END) OVER (PARTITION BY load_date, branch_name) AS amt_total_13,
        MAX(CASE WHEN line = '7240' THEN final_amount ELSE 0 END) OVER (PARTITION BY load_date, branch_name) AS amt_7240,
        MAX(CASE WHEN line = '7045' THEN final_amount ELSE 0 END) OVER (PARTITION BY load_date, branch_name) AS amt_7045
    FROM final_join
)

SELECT
    load_date, branch_name, order_id, code, description,
    CASE WHEN order_id = 82 THEN (amt_total_13 - (amt_7240 + amt_7045)) ELSE final_amount END AS amount,
    final_accounts AS total_accounts,
    CONCAT(
        CAST(CAST(SUBSTR(CAST(load_date AS VARCHAR), 9, 2) AS INTEGER) AS VARCHAR),
        CASE
            WHEN CAST(SUBSTR(CAST(load_date AS VARCHAR), 9, 2) AS INTEGER) IN (11,12,13) THEN 'th'
            WHEN MOD(CAST(SUBSTR(CAST(load_date AS VARCHAR), 9, 2) AS INTEGER), 10) = 1 THEN 'st'
            WHEN MOD(CAST(SUBSTR(CAST(load_date AS VARCHAR), 9, 2) AS INTEGER), 10) = 2 THEN 'nd'
            WHEN MOD(CAST(SUBSTR(CAST(load_date AS VARCHAR), 9, 2) AS INTEGER), 10) = 3 THEN 'rd'
            ELSE 'th'
        END, ' ',
        CASE CAST(SUBSTR(CAST(load_date AS VARCHAR), 6, 2) AS INTEGER)
            WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March' WHEN 4 THEN 'April'
            WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August'
            WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
        END, ' ',
        SUBSTR(CAST(load_date AS VARCHAR), 1, 4)
    ) AS load_date_label,
    CURRENT_TIMESTAMP AS updated_at
FROM calculated_difference
ORDER BY branch_name, order_id
