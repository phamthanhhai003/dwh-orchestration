{{ config(
    materialized='table',
    unique_key=['load_date', 'ratio_id'],
    incremental_strategy='merge',
    database='hive',
    schema='gold',
    tags=['ratio', 'reporting']
) }}

-- Division: ratio (Financial Ratios — CAMEL's & Liquidity/Solvability)
-- Strategy: derived (computed from existing gold models, no seeds, no staging)
-- Sources: final_po_soc_report (balance sheet) + final_po_sie_report (P&L)
-- Template: 'RATIO' sheet in docs/report-template/consolidate.xlsx
-- Business doc: docs/business-documentation/ratio.md
-- Grain: 1 row per (load_date, ratio_id)
--
-- All cell references from the Excel RATIO sheet are mapped to PO SOC/PO SIE
-- order_ids. See business doc for the complete mapping table.
--
-- LCR (Liquidity Coverage Ratio) excluded — requires SOC column S data and
-- has #REF! errors in the template.

WITH

-- ── Pull required values from PO SOC (balance sheet) ──────────────────────────
soc_vals AS (
    SELECT
        load_date,
        CAST(SUM(CASE WHEN order_id =   9 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS liquid_funds,         -- SOC D8
        CAST(SUM(CASE WHEN order_id =  13 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS items_in_collection,  -- SOC D12
        CAST(SUM(CASE WHEN order_id =  14 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS comm_banks,           -- SOC D13
        CAST(SUM(CASE WHEN order_id =  21 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS current_loans,        -- SOC D19
        CAST(SUM(CASE WHEN order_id =  22 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS seasonal_crop,        -- SOC D21
        CAST(SUM(CASE WHEN order_id =  24 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS project_loan,         -- SOC D23
        CAST(SUM(CASE WHEN order_id =  28 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS microfinance,         -- SOC D27
        CAST(SUM(CASE WHEN order_id =  37 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS past_due,             -- SOC D35
        CAST(SUM(CASE WHEN order_id =  54 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS provision,            -- SOC D51
        CAST(SUM(CASE WHEN order_id =  58 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS receivable,           -- SOC D54
        CAST(SUM(CASE WHEN order_id =  62 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS fixed_assets,         -- SOC D58
        CAST(SUM(CASE WHEN order_id =  77 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS other_assets,         -- SOC D73
        CAST(SUM(CASE WHEN order_id =  94 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS total_assets,         -- SOC D90
        CAST(SUM(CASE WHEN order_id = 106 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS deposits,             -- SOC D102
        CAST(SUM(CASE WHEN order_id = 109 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS gov_dd,               -- SOC D105
        CAST(SUM(CASE WHEN order_id = 110 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS biz_dd,               -- SOC D106
        CAST(SUM(CASE WHEN order_id = 114 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS gov_td,               -- SOC D110
        CAST(SUM(CASE WHEN order_id = 115 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS others_td,            -- SOC D111
        CAST(SUM(CASE WHEN order_id = 127 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS hnf,                  -- SOC D123
        CAST(SUM(CASE WHEN order_id = 144 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS bank_garansi,         -- SOC D140
        CAST(SUM(CASE WHEN order_id = 150 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS capital_accounts,     -- SOC D146
        CAST(SUM(CASE WHEN order_id = 151 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS provisioning,         -- SOC D147
        CAST(SUM(CASE WHEN order_id = 152 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS capital_paid_up,      -- SOC D148
        CAST(SUM(CASE WHEN order_id = 153 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS capital_reserve,      -- SOC D149
        CAST(SUM(CASE WHEN order_id = 154 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS profits_losses        -- SOC D150
    FROM {{ ref('final_po_soc_report') }}
    {% if var("target_date", "") != "" %}
    WHERE load_date = DATE '{{ var("target_date") }}'
    {% endif %}
    GROUP BY load_date
),

-- ── Pull required values from PO SIE (P&L) ───────────────────────────────────
sie_vals AS (
    SELECT
        load_date,
        CAST(SUM(CASE WHEN order_id =   9 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS int_due_from_banks,   -- SIE C8
        CAST(SUM(CASE WHEN order_id =  13 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS int_income_loans,     -- SIE C12
        CAST(SUM(CASE WHEN order_id =  62 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS total_op_income,      -- SIE C62
        CAST(SUM(CASE WHEN order_id =  64 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS interest_expense,     -- SIE C64
        CAST(SUM(CASE WHEN order_id =  65 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS int_indiv_deposits,   -- SIE C65
        CAST(SUM(CASE WHEN order_id = 139 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS total_op_expenses,    -- SIE C138
        CAST(SUM(CASE WHEN order_id = 140 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS net_income_before_tax,-- SIE C139
        CAST(SUM(CASE WHEN order_id = 142 THEN amount ELSE 0 END) AS DECIMAL(18,2)) AS net_income_after_tax  -- SIE C141
    FROM {{ ref('final_po_sie_report') }}
    {% if var("target_date", "") != "" %}
    WHERE load_date = DATE '{{ var("target_date") }}'
    {% endif %}
    GROUP BY load_date
),

-- ── Derived intermediate values ───────────────────────────────────────────────
derived AS (
    SELECT
        s.load_date,
        -- SOC values
        s.liquid_funds,
        s.items_in_collection,
        s.comm_banks,
        s.current_loans,
        s.past_due,
        s.provision,
        s.receivable,
        s.fixed_assets,
        s.other_assets,
        s.total_assets,
        s.deposits,
        s.capital_accounts,
        s.provisioning,
        s.capital_paid_up,
        s.capital_reserve,
        s.profits_losses,
        -- Gross loans (Current + Past Due, WITHOUT provision)
        CAST(s.current_loans + s.past_due AS DECIMAL(18,2)) AS gross_loans,
        -- Equity capital (Capital Paid-Up + Reserve + Profits)
        CAST(s.capital_paid_up + s.capital_reserve + s.profits_losses AS DECIMAL(18,2)) AS equity_capital,
        -- LDR denominator: (Deposits - Gov DD - Gov TD) + Bank Garansi
        CAST((s.deposits - s.gov_dd - s.gov_td) + s.bank_garansi AS DECIMAL(18,2)) AS ldr_denom,
        -- Cash Ratio denominator: Seasonal Crop + Project + Microfinance
        CAST(s.seasonal_crop + s.project_loan + s.microfinance AS DECIMAL(18,2)) AS cr_denom,
        -- ATMR (Risk-Weighted Assets for CAR)
        CAST(
            CAST(0.2 AS DECIMAL(18,2)) * (s.items_in_collection + s.comm_banks)
            + s.current_loans + s.past_due + s.provision
            + s.fixed_assets + s.receivable + s.other_assets
        AS DECIMAL(18,2)) AS atmr,
        -- CAR capital (excludes Capital Reserve per Commercial Bank note)
        CAST(s.capital_paid_up + s.profits_losses AS DECIMAL(18,2)) AS car_capital,
        -- Capital Ratio numerator: Capital Accounts - Provision + Provisioning
        CAST(s.capital_accounts - s.provision + s.provisioning AS DECIMAL(18,2)) AS cr2_numer,
        -- IER denominator: Biz DD + Others TD + HNF
        CAST(s.biz_dd + s.others_td + s.hnf AS DECIMAL(18,2)) AS ier_denom,
        -- Earning assets (Gross Loans + Commercial Banks)
        CAST(s.current_loans + s.past_due + s.comm_banks AS DECIMAL(18,2)) AS earning_assets,
        -- RAR denominator: Total Assets - Liquid Funds
        CAST(s.total_assets - s.liquid_funds AS DECIMAL(18,2)) AS rar_denom,
        -- Negative provision (for AQ numerator) — use multiply to avoid Dremio widening
        CAST(s.provision * CAST(-1 AS INT) AS DECIMAL(18,2)) AS neg_provision,
        -- Zero constant for interbank
        CAST(0 AS DECIMAL(18,2)) AS zero_val,
        -- Variant LDR denominator
        CAST((s.deposits - s.gov_dd - s.gov_td) + s.bank_garansi
             - s.gov_dd - CAST(175000000 AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS variant_ldr_denom,
        -- SIE values
        p.int_due_from_banks,
        p.int_income_loans,
        p.total_op_income,
        p.interest_expense,
        p.int_indiv_deposits,
        p.total_op_expenses,
        p.net_income_before_tax,
        p.net_income_after_tax,
        -- NIM numerator: (Int Banks + Int Loans) - Int Individual Deposits
        CAST((p.int_due_from_banks + p.int_income_loans) - p.int_indiv_deposits AS DECIMAL(18,2)) AS nim_numer
    FROM soc_vals s
    JOIN sie_vals p ON s.load_date = p.load_date
),

-- ── Compute all ratios ────────────────────────────────────────────────────────
-- NOTE: Every numerator/denominator is explicitly CAST to DECIMAL(18,2) to
-- satisfy Dremio's strict UNION ALL type matching requirements.
ratios AS (
    -- Section 1: Liquidity Ratios
    SELECT load_date, CAST('r01' AS VARCHAR) AS ratio_id, CAST('QR' AS VARCHAR) AS ratio_code,
           CAST('Quick Ratio' AS VARCHAR) AS ratio_name, CAST(1 AS INT) AS section,
           CAST(liquid_funds AS DECIMAL(18,2)) AS numerator, CAST(deposits AS DECIMAL(18,2)) AS denominator,
           CAST(liquid_funds / NULLIF(deposits, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4)) AS ratio_pct
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r02' AS VARCHAR), CAST('BK' AS VARCHAR),
           CAST('Banking Ratio' AS VARCHAR), CAST(1 AS INT),
           CAST(gross_loans AS DECIMAL(18,2)), CAST(deposits AS DECIMAL(18,2)),
           CAST(gross_loans / NULLIF(deposits, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r03' AS VARCHAR), CAST('ALR' AS VARCHAR),
           CAST('Assets to Loan Ratio' AS VARCHAR), CAST(1 AS INT),
           CAST(gross_loans AS DECIMAL(18,2)), CAST(total_assets AS DECIMAL(18,2)),
           CAST(gross_loans / NULLIF(total_assets, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r04' AS VARCHAR), CAST('CR' AS VARCHAR),
           CAST('Cash Ratio' AS VARCHAR), CAST(1 AS INT),
           CAST(liquid_funds AS DECIMAL(18,2)), CAST(cr_denom AS DECIMAL(18,2)),
           CAST(liquid_funds / NULLIF(cr_denom, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r05' AS VARCHAR), CAST('LDR' AS VARCHAR),
           CAST('Loan to Deposit Ratio' AS VARCHAR), CAST(1 AS INT),
           CAST(gross_loans AS DECIMAL(18,2)), CAST(ldr_denom AS DECIMAL(18,2)),
           CAST(gross_loans / NULLIF(ldr_denom, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    -- Section 2: Solvability Ratios
    UNION ALL
    SELECT load_date, CAST('r06' AS VARCHAR), CAST('PR' AS VARCHAR),
           CAST('Primary Ratio' AS VARCHAR), CAST(2 AS INT),
           CAST(equity_capital AS DECIMAL(18,2)), CAST(total_assets AS DECIMAL(18,2)),
           CAST(equity_capital / NULLIF(total_assets, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r07' AS VARCHAR), CAST('RAR' AS VARCHAR),
           CAST('Risk Assets Ratio' AS VARCHAR), CAST(2 AS INT),
           CAST(equity_capital AS DECIMAL(18,2)), CAST(rar_denom AS DECIMAL(18,2)),
           CAST(equity_capital / NULLIF(rar_denom, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r08' AS VARCHAR), CAST('CR2' AS VARCHAR),
           CAST('Capital Ratio' AS VARCHAR), CAST(2 AS INT),
           CAST(cr2_numer AS DECIMAL(18,2)), CAST(gross_loans AS DECIMAL(18,2)),
           CAST(cr2_numer / NULLIF(gross_loans, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    -- Section 3: CAMEL's Ratios
    UNION ALL
    SELECT load_date, CAST('r09' AS VARCHAR), CAST('CAR' AS VARCHAR),
           CAST('Capital Adequacy Ratio' AS VARCHAR), CAST(3 AS INT),
           CAST(car_capital AS DECIMAL(18,2)), CAST(atmr AS DECIMAL(18,2)),
           CAST(car_capital / NULLIF(atmr, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r10' AS VARCHAR), CAST('NPL' AS VARCHAR),
           CAST('Non-Performing Loan Ratio' AS VARCHAR), CAST(3 AS INT),
           CAST(past_due AS DECIMAL(18,2)), CAST(gross_loans AS DECIMAL(18,2)),
           CAST(past_due / NULLIF(gross_loans, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r11' AS VARCHAR), CAST('AQ' AS VARCHAR),
           CAST('Asset Quality' AS VARCHAR), CAST(3 AS INT),
           CAST(neg_provision AS DECIMAL(18,2)), CAST(earning_assets AS DECIMAL(18,2)),
           CAST(neg_provision / NULLIF(earning_assets, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r12' AS VARCHAR), CAST('NPM' AS VARCHAR),
           CAST('Net Profit Margin' AS VARCHAR), CAST(3 AS INT),
           CAST(net_income_after_tax AS DECIMAL(18,2)), CAST(total_op_income AS DECIMAL(18,2)),
           CAST(net_income_after_tax / NULLIF(total_op_income, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r13' AS VARCHAR), CAST('NIM' AS VARCHAR),
           CAST('Net Interest Margin' AS VARCHAR), CAST(3 AS INT),
           CAST(nim_numer AS DECIMAL(18,2)), CAST(earning_assets AS DECIMAL(18,2)),
           CAST(nim_numer / NULLIF(earning_assets, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r14' AS VARCHAR), CAST('ROA' AS VARCHAR),
           CAST('Return on Assets' AS VARCHAR), CAST(3 AS INT),
           CAST(net_income_before_tax AS DECIMAL(18,2)), CAST(total_assets AS DECIMAL(18,2)),
           CAST(net_income_before_tax / NULLIF(total_assets, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r15' AS VARCHAR), CAST('BOPO' AS VARCHAR),
           CAST('Cost-to-Income Ratio (BOPO)' AS VARCHAR), CAST(3 AS INT),
           CAST(total_op_expenses AS DECIMAL(18,2)), CAST(total_op_income AS DECIMAL(18,2)),
           CAST(total_op_expenses / NULLIF(total_op_income, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r16' AS VARCHAR), CAST('ROE' AS VARCHAR),
           CAST('Return on Equity' AS VARCHAR), CAST(3 AS INT),
           CAST(net_income_after_tax AS DECIMAL(18,2)), CAST(equity_capital AS DECIMAL(18,2)),
           CAST(net_income_after_tax / NULLIF(equity_capital, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r17' AS VARCHAR), CAST('LDR_C' AS VARCHAR),
           CAST('LDR (CAMEL)' AS VARCHAR), CAST(3 AS INT),
           CAST(gross_loans AS DECIMAL(18,2)), CAST(ldr_denom AS DECIMAL(18,2)),
           CAST(gross_loans / NULLIF(ldr_denom, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r18' AS VARCHAR), CAST('IBK' AS VARCHAR),
           CAST('Interbank Net Obligation' AS VARCHAR), CAST(3 AS INT),
           CAST(zero_val AS DECIMAL(18,2)), CAST(liquid_funds AS DECIMAL(18,2)),
           CAST(0 AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r19' AS VARCHAR), CAST('IER' AS VARCHAR),
           CAST('Interest Expense Ratio' AS VARCHAR), CAST(3 AS INT),
           CAST(interest_expense AS DECIMAL(18,2)), CAST(ier_denom AS DECIMAL(18,2)),
           CAST(interest_expense / NULLIF(ier_denom, CAST(0 AS DECIMAL(18,2))) * 100 AS DECIMAL(18,4))
    FROM derived

    -- Section 4: Other
    UNION ALL
    SELECT load_date, CAST('r20' AS VARCHAR), CAST('AT' AS VARCHAR),
           CAST('Asset Turnover' AS VARCHAR), CAST(4 AS INT),
           CAST(total_op_income AS DECIMAL(18,2)), CAST(total_assets AS DECIMAL(18,2)),
           CAST(total_op_income / NULLIF(total_assets, CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,4))
    FROM derived

    UNION ALL
    SELECT load_date, CAST('r21' AS VARCHAR), CAST('VLDR' AS VARCHAR),
           CAST('Variant LDR' AS VARCHAR), CAST(4 AS INT),
           CAST(gross_loans AS DECIMAL(18,2)), CAST(variant_ldr_denom AS DECIMAL(18,2)),
           CAST(gross_loans / NULLIF(variant_ldr_denom, CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,4))
    FROM derived
)

SELECT
    load_date,
    ratio_id,
    ratio_code,
    ratio_name,
    section,
    CAST(numerator   AS DECIMAL(18,2)) AS numerator,
    CAST(denominator AS DECIMAL(18,2)) AS denominator,
    ratio_pct,
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
FROM ratios

