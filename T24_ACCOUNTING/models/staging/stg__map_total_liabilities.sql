{{ config(materialized='table', schema='silver') }}

-- Summary/total rows cho LIABILITIES report (13 dòng tổng). Migrate từ ACCOUNTING_REPORTS:
--   * amount        : trial_balance header rows (closing_balance) theo account_code.
--   * total_accounts: ĐỔI nguồn → acct_count_liabilities (CRB), CHỈ nhóm deposit
--     (DEMAND/TIME/PASSBOOK). Nhóm khác (capital/pnl/loan-fee) KHÔNG có account ở CRB → 0.
-- ⚠️ acct_count_liabilities key theo co_code → bridge co_code→branch_name (từ trial_balance).

{% set run_date = var('target_date') %}

WITH seed_data AS (
    SELECT CAST(account_code AS VARCHAR) AS account_code, parent_group
    FROM {{ ref('map_total_liabilities') }}
),

branch_co AS (   -- bridge: co_code ↔ branch_name (trial_balance có cả 2)
    SELECT DISTINCT branch_name, co_code
    FROM hive.silver.trial_balance_detail
    WHERE co_code IS NOT NULL AND load_date = DATE '{{ run_date }}'
),

amt_base AS (
    SELECT s.load_date, s.branch_name, m.parent_group, SUM(s.closing_balance) AS amt
    FROM hive.silver.trial_balance_detail s
    JOIN seed_data m ON CAST(s.original_line AS VARCHAR) = m.account_code
    WHERE s.load_date = DATE '{{ run_date }}' AND s.statement_type = 'GL'
    GROUP BY 1, 2, 3
),

acc_base AS (
    SELECT a.load_date, bc.branch_name, m.parent_group, SUM(a.total_accounts) AS acc
    FROM {{ ref('acct_count_liabilities') }} a
    JOIN branch_co bc ON a.co_code = bc.co_code
    JOIN seed_data m ON a.line = m.account_code
    WHERE a.load_date = DATE '{{ run_date }}'
    GROUP BY 1, 2, 3
),

base_balances AS (
    SELECT
        COALESCE(am.load_date, ac.load_date)       AS load_date,
        COALESCE(am.branch_name, ac.branch_name)   AS branch_name,
        COALESCE(am.parent_group, ac.parent_group) AS parent_group,
        COALESCE(am.amt, 0)                        AS amt,
        COALESCE(ac.acc, 0)                        AS acc
    FROM amt_base am
    FULL OUTER JOIN acc_base ac
      ON am.load_date = ac.load_date AND am.branch_name = ac.branch_name AND am.parent_group = ac.parent_group
),

group_sums AS (
    SELECT
        load_date,
        branch_name,
        SUM(CASE WHEN parent_group = 'DEMAND_DEPOSIT'     THEN amt ELSE 0 END) AS amt_2,
        SUM(CASE WHEN parent_group = 'TIME_DEPOSIT'       THEN amt ELSE 0 END) AS amt_3,
        SUM(CASE WHEN parent_group = 'PASSBOOK_SAVING'    THEN amt ELSE 0 END) AS amt_4,
        SUM(CASE WHEN parent_group = 'SUNDRY_OTHERS'      THEN amt ELSE 0 END) AS amt_sundry_raw,
        SUM(CASE WHEN parent_group = 'INTEREST_ACCRUED'   THEN amt ELSE 0 END) AS amt_6,
        SUM(CASE WHEN parent_group = 'RESTRICTED_DEPOSIT' THEN amt ELSE 0 END) AS amt_7,
        SUM(CASE WHEN parent_group = 'UNEARNED_BASE'      THEN amt ELSE 0 END) AS amt_unearned_base,
        SUM(CASE WHEN parent_group = 'LOAN_FEE_ADV'       THEN amt ELSE 0 END) AS amt_9,
        SUM(CASE WHEN parent_group = 'DUE_TO_BRANCH'      THEN amt ELSE 0 END) AS amt_10,
        SUM(CASE WHEN parent_group = 'PROVISIONING'       THEN amt ELSE 0 END) AS amt_prov,
        SUM(CASE WHEN parent_group = 'CAPITAL_BASE'       THEN amt ELSE 0 END) AS amt_cap_base,
        SUM(CASE WHEN parent_group = 'PNL'                THEN amt ELSE 0 END) AS amt_12,
        SUM(CASE WHEN parent_group = 'DEMAND_DEPOSIT'     THEN acc ELSE 0 END) AS acc_2,
        SUM(CASE WHEN parent_group = 'TIME_DEPOSIT'       THEN acc ELSE 0 END) AS acc_3,
        SUM(CASE WHEN parent_group = 'PASSBOOK_SAVING'    THEN acc ELSE 0 END) AS acc_4,
        SUM(CASE WHEN parent_group = 'LOAN_FEE_ADV'       THEN acc ELSE 0 END) AS acc_9,
        SUM(CASE WHEN parent_group = 'CAPITAL_BASE'       THEN acc ELSE 0 END) AS acc_cap_base,
        SUM(CASE WHEN parent_group = 'PNL'                THEN acc ELSE 0 END) AS acc_12,
        SUM(CASE WHEN parent_group = 'PROVISIONING'       THEN acc ELSE 0 END) AS acc_prov
    FROM base_balances
    GROUP BY 1, 2
),

calculated_levels AS (
    SELECT *,
        (amt_2 + amt_3 + amt_4) AS amt_1,
        (acc_2 + acc_3 + acc_4) AS acc_1,
        (amt_unearned_base + amt_9) AS amt_8,
        (amt_sundry_raw + amt_6 + amt_7 + amt_unearned_base + amt_9) AS amt_5,
        (amt_prov + amt_cap_base + amt_12) AS amt_11,
        (acc_prov + acc_cap_base + acc_12) AS acc_11
    FROM group_sums
),

final_aggregation AS (
    SELECT *,
        (amt_1 + amt_5 + amt_10
            + CASE WHEN branch_name IN ('Head Office', 'RE000010_03_SUKURSAL_GLENO')
                   THEN amt_11 ELSE (amt_prov + amt_12) END) AS amt_13,
        (acc_1 + acc_9 + acc_11) AS acc_13
    FROM calculated_levels
)

SELECT load_date, branch_name, 1 as stt, 'DEPOSITS (SUM)' as description, amt_1 as amount, acc_1 as total_accounts FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 2, 'DemandDeposit (SUM)', amt_2, acc_2 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 3, 'TimeDeposits (SUM)', amt_3, acc_3 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 4, 'Other(PassbookSaving) (SUM)', amt_4, acc_4 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 5, 'OtherSundryCurrentLiabilit. (SUM)', amt_5, 0 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 6, 'InterestAccrued (SUM)', amt_6, 0 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 7, 'RESTRICTEDDEPOSIT (SUM)', amt_7, 0 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 8, 'UnearnedInterest (SUM)', amt_8, acc_9 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 9, 'LoanCollec.FeeinAdvance (SUM)', amt_9, acc_9 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 10, 'OtherLiabilit/DueToBranch (SUM)', amt_10, 0 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 11, 'CAPITALACCOUNTS: (SUM)', amt_11, acc_11 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 12, 'Profits/Losses (SUM)', amt_12, acc_12 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 13, 'TotalLiabilities&Capital (SUM)', amt_13, acc_13 FROM final_aggregation
