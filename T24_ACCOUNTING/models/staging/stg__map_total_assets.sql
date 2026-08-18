{{ config(materialized='table', schema='silver') }}

-- Summary/total rows cho ASSET report (19 dòng tổng: LIQUID FUNDS SUM ... Total Assets SUM).
-- Migrate từ ACCOUNTING_REPORTS: GIỮ NGUYÊN rollup amount, NHƯNG total_accounts (acc) đổi
-- nguồn: KHÔNG còn SUM(trial_balance.total_accounts) — lấy từ acct_count_assets (CRIS) theo line.
-- amount: trial_balance header rows (closing_balance) join seed map_total_assets theo account_code.
-- acc   : acct_count_assets (chỉ loan line có số) join seed theo line → rollup parent_group.

{% set run_date = var('target_date') %}

WITH seed_data AS (
    SELECT CAST(account_code AS VARCHAR) AS account_code, account_name, parent_group
    FROM {{ ref('map_total_assets') }}
),

amt_base AS (
    SELECT s.load_date, s.branch_name, m.parent_group, SUM(s.closing_balance) AS amt
    FROM hive.silver.trial_balance_detail s
    JOIN seed_data m ON CAST(s.original_line AS VARCHAR) = m.account_code
    WHERE s.load_date = DATE '{{ run_date }}' AND s.statement_type = 'GL'
    GROUP BY 1, 2, 3
),

acc_base AS (
    SELECT a.load_date, a.branch_name, m.parent_group, SUM(a.total_accounts) AS acc
    FROM {{ ref('acct_count_assets') }} a
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
      ON am.load_date = ac.load_date
     AND am.branch_name = ac.branch_name
     AND am.parent_group = ac.parent_group
),

group_sums AS (
    SELECT
        load_date,
        branch_name,
        SUM(CASE WHEN parent_group = 'LIQUID_FUNDS'     THEN amt ELSE 0 END) AS amt_raw_liquid,
        SUM(CASE WHEN parent_group = 'CALL_DEPOSITS'    THEN amt ELSE 0 END) AS amt_call_dep,
        SUM(CASE WHEN parent_group = 'OTHERS_DOMESTIC'  THEN amt ELSE 0 END) AS amt_other_dom,
        SUM(CASE WHEN parent_group = 'FOREIGN_CURRENCY' THEN amt ELSE 0 END) AS amt_foreign_curr,
        SUM(CASE WHEN parent_group = 'INVESTMENT'       THEN amt ELSE 0 END) AS amt_investment,
        SUM(CASE WHEN parent_group = 'CURRENT_LOAN'     THEN amt ELSE 0 END) AS amt_curr_loan,
        SUM(CASE WHEN parent_group = 'PAST_DUE'         THEN amt ELSE 0 END) AS amt_past_due,
        SUM(CASE WHEN parent_group = 'PROVISION'        THEN amt ELSE 0 END) AS amt_provision,
        SUM(CASE WHEN parent_group = 'ACC_RECEIVABLE'   THEN amt ELSE 0 END) AS amt_acc_receiv,
        SUM(CASE WHEN parent_group = 'FIXED_ASSETS'     THEN amt ELSE 0 END) AS amt_fixed_raw,
        SUM(CASE WHEN parent_group = 'RESERVE_DEPR'     THEN amt ELSE 0 END) AS amt_depr,
        SUM(CASE WHEN parent_group = 'OTHER_ASSETS'     THEN amt ELSE 0 END) AS amt_other_assets_raw,
        SUM(CASE WHEN parent_group = 'OFFICE_ACCOUNTS'  THEN amt ELSE 0 END) AS amt_office_acc,
        SUM(CASE WHEN parent_group = 'ASSET_HELD_DEBT'  THEN amt ELSE 0 END) AS amt_asset_held,
        SUM(CASE WHEN parent_group = 'MISC_ASSETS'      THEN amt ELSE 0 END) AS amt_misc_assets,

        SUM(CASE WHEN parent_group = 'LIQUID_FUNDS'     THEN acc ELSE 0 END) AS acc_raw_liquid,
        SUM(CASE WHEN parent_group = 'CALL_DEPOSITS'    THEN acc ELSE 0 END) AS acc_call_dep,
        SUM(CASE WHEN parent_group = 'OTHERS_DOMESTIC'  THEN acc ELSE 0 END) AS acc_other_dom,
        SUM(CASE WHEN parent_group = 'FOREIGN_CURRENCY' THEN acc ELSE 0 END) AS acc_foreign_curr,
        SUM(CASE WHEN parent_group = 'INVESTMENT'       THEN acc ELSE 0 END) AS acc_investment,
        SUM(CASE WHEN parent_group = 'CURRENT_LOAN'     THEN acc ELSE 0 END) AS acc_curr_loan,
        SUM(CASE WHEN parent_group = 'PAST_DUE'         THEN acc ELSE 0 END) AS acc_past_due,
        SUM(CASE WHEN parent_group = 'PROVISION'        THEN acc ELSE 0 END) AS acc_provision,
        SUM(CASE WHEN parent_group = 'ACC_RECEIVABLE'   THEN acc ELSE 0 END) AS acc_acc_receiv,
        SUM(CASE WHEN parent_group = 'FIXED_ASSETS'     THEN acc ELSE 0 END) AS acc_fixed_raw,
        SUM(CASE WHEN parent_group = 'RESERVE_DEPR'     THEN acc ELSE 0 END) AS acc_depr,
        SUM(CASE WHEN parent_group = 'OTHER_ASSETS'     THEN acc ELSE 0 END) AS acc_other_assets_raw,
        SUM(CASE WHEN parent_group = 'OFFICE_ACCOUNTS'  THEN acc ELSE 0 END) AS acc_office_acc,
        SUM(CASE WHEN parent_group = 'ASSET_HELD_DEBT'  THEN acc ELSE 0 END) AS acc_asset_held,
        SUM(CASE WHEN parent_group = 'MISC_ASSETS'      THEN acc ELSE 0 END) AS acc_misc_assets
    FROM base_balances
    GROUP BY 1, 2
),

calculated_levels AS (
    SELECT
        *,
        (amt_call_dep + amt_other_dom)                                  AS amt_3,
        (amt_call_dep + amt_other_dom + amt_foreign_curr)              AS amt_2,
        (amt_raw_liquid + amt_call_dep + amt_other_dom + amt_foreign_curr) AS amt_1,
        (amt_curr_loan + amt_past_due + amt_provision)                 AS amt_8,
        (amt_fixed_raw + amt_depr)                                      AS amt_13,
        (amt_other_assets_raw + amt_office_acc + amt_asset_held + amt_misc_assets) AS amt_15,

        (acc_call_dep + acc_other_dom)                                  AS acc_3,
        (acc_call_dep + acc_other_dom + acc_foreign_curr)              AS acc_2,
        (acc_raw_liquid + acc_call_dep + acc_other_dom + acc_foreign_curr) AS acc_1,
        (acc_curr_loan + acc_past_due + acc_provision)                 AS acc_8,
        (acc_fixed_raw + acc_depr)                                      AS acc_13,
        (acc_other_assets_raw + acc_office_acc + acc_asset_held + acc_misc_assets) AS acc_15
    FROM group_sums
),

final_aggregation AS (
    SELECT
        *,
        (amt_1 + amt_8 + amt_acc_receiv + amt_13 + amt_15 +
            CASE WHEN branch_name IN ('Head Office', 'RE000010_03_SUKURSAL_GLENO') THEN amt_investment ELSE 0 END) AS amt_19,
        (acc_1 + acc_8 + acc_acc_receiv + acc_13 + acc_15 +
            CASE WHEN branch_name IN ('Head Office', 'RE000010_03_SUKURSAL_GLENO') THEN acc_investment ELSE 0 END) AS acc_19
    FROM calculated_levels
)

SELECT load_date, branch_name, 1 as stt, 'LIQUID FUNDS (SUM)' as description, amt_1 as amount, acc_1 as total_accounts FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 2, 'DuefromCom.Banks (SUM)', amt_2, acc_2 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 3, 'DomesticCurrency', amt_3, acc_3 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 4, 'CallDeposits (SUM)', amt_call_dep, acc_call_dep FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 5, 'Other (SUM)', amt_other_dom, acc_other_dom FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 6, 'ForeignCurrency (SUM)', amt_foreign_curr, acc_foreign_curr FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 7, 'INVESTMENT (SUM)', amt_investment, acc_investment FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 8, 'LOAN,ADV.&DISC. (SUM)', amt_8, acc_8 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 9, 'CURRENTLOAN (SUM)', amt_curr_loan, acc_curr_loan FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 10, 'PASTDUELOANS (SUM)', amt_past_due, acc_past_due FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 11, 'PROV.FORL.LOSSES (SUM)', amt_provision, acc_provision FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 12, 'ACC.RECEIVABLE (SUM)', amt_acc_receiv, acc_acc_receiv FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 13, 'FIXEDASSETS (SUM)', amt_13, acc_13 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 14, 'RESERVEFORDEPREC. (SUM)', amt_depr, acc_depr FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 15, 'OTHERASSETS (SUM)', amt_15, acc_15 FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 16, 'OfficeAccounts (SUM)', amt_office_acc, acc_office_acc FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 17, 'AssetHeldInRes.ofDebt (SUM)', amt_asset_held, acc_asset_held FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 18, 'MiscellaneousAssets (SUM)', amt_misc_assets, acc_misc_assets FROM final_aggregation
UNION ALL SELECT load_date, branch_name, 19, 'Total Assets (SUM)', amt_19, acc_19 FROM final_aggregation
