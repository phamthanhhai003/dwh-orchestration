{{ config(
    materialized='table',
    unique_key=['report_month', 'code', 'description'],
    incremental_strategy='merge',
    database='hive',
    schema='gold',
    tags=['soc', 'monthly', 'balance_sheet_assets']
) }}

WITH report_mapping AS (

    SELECT '1.1.1' AS code, 'Cash' AS description, 'Cash in Vault' AS source_label UNION ALL
    SELECT '1.1.1', 'Cash', 'Cash in ATM Vault' UNION ALL
    SELECT '1.1.2', 'Due from Central Bank', 'Due from Central Bank' UNION ALL
    SELECT '1.1.3', 'Items in Course of Collection', 'Items in course of collect.' UNION ALL
    SELECT '1.1.4', 'Due from Commercial Banks', 'Due from Com. Banks' UNION ALL
    SELECT '1.2', 'INVESTMENT', 'INVESTMENT' UNION ALL
    SELECT '1.2.1', 'Domestic Currency Investments', 'Domestic Currency Investments' UNION ALL
    SELECT '1.2.2', 'Foreign Currency Investments', 'Foreign Currency Investments' UNION ALL
    SELECT '1.2.3', 'Less: Provision for Investment Losses', 'Less: Provision for Investment Losses' UNION ALL

    SELECT '1.3', 'SECURITIES PURCHASED WITH A VIEW TO RESALE', 'SECURITIES PURCHASED WITH A VIEW TO RESALE' UNION ALL
    SELECT '1.3.1', 'From Central Bank', 'From Central Bank' UNION ALL
    SELECT '1.3.2', 'Local', 'Local' UNION ALL
    SELECT '1.3.3', 'Foreign', 'Foreign' UNION all

    SELECT '1.4.1', 'Standard Loans', 'CURRENT LOAN' UNION ALL
    SELECT '1.4.2', 'Past due Loans', 'PAST DUE LOANS' UNION ALL
    SELECT '1.4.3', 'Less: Provision for Loan Losses', 'PROV. FOR L. LOSSES' UNION ALL


    SELECT '1.5.1', 'Interest Accrued', 'DueFr.Banks&OtherFin.INst' UNION ALL
    SELECT '1.5.1', 'Interest Accrued', 'PerformingLoans' UNION ALL
    SELECT '1.5.1', 'Interest Accrued', 'Non-PerformingLoans' UNION ALL
    SELECT '1.5.2', 'Commissions', 'DUMMY_LABEL' UNION ALL
    SELECT '1.5.3', 'Other Receivables', 'OtherReceivable' UNION ALL

    SELECT '1.5.4', 'Less: Provision for Losses on Receivables', 'Less: Provision for Losses on Receivables' UNION ALL


    SELECT '1.6.2', 'Premises', 'Offices/Buildings' UNION ALL
    SELECT '1.6.2', 'Premises', 'OfficeUnderCunstruction' UNION ALL
    SELECT '1.6.2', 'Premises', 'LeaseholdImprov.' UNION ALL
    SELECT '1.6.3', 'Furniture and Fixtures', 'FurnetureandFixtures' UNION ALL
    SELECT '1.6.4', 'Machinery Equipment', 'MachineryandEquip.' UNION ALL
    SELECT '1.6.5', 'Vehicles', 'Vehicles' UNION ALL
    SELECT '1.6.6', 'Non Physical Assets', 'NonPhysicalAsset' UNION ALL
    SELECT '1.6.9', 'Less: Reserve for Depreciation', 'RESERVE FOR DEPREC.' UNION ALL


    SELECT '1.9.1', 'Prepaid Expenses', 'PrepaidExpenses' UNION ALL
    SELECT '1.9.2', 'Inter Branch Transactions (Net)', 'Domestic Currency' UNION ALL
    SELECT '1.9.2', 'Inter Branch Transactions (Net)', 'Foreign Currency' UNION ALL
    SELECT '1.9.3', 'Office Accounts', 'OfficeAccounts' UNION ALL
    SELECT '1.9.4', 'Assets Held in Respect of Debt Satisfaction', 'AssetHeldInRes.ofDebt' UNION ALL
    SELECT '1.9.6', 'Miscellaneous Assets', 'MiscellaneousAssets'
),

raw_data AS (
    SELECT
        report_month,
        description,
        SUM(amount) AS total_amount
    FROM {{ ref('soc_assets_real_v2') }}
    {% if var("target_date", "") != "" %}
    WHERE report_month = '{{ var("target_date")[0:7] }}'
    {% elif is_incremental() %}
    WHERE report_month >= (
        SELECT COALESCE(MAX(report_month), '1900-01')
        FROM {{ this }}
    )
    {% endif %}
    GROUP BY 1, 2
),

all_periods AS (
    SELECT DISTINCT report_month FROM raw_data
),

report_skeleton AS (
    SELECT m.*, p.report_month
    FROM report_mapping m
    CROSS JOIN all_periods p
),

detailed_report AS (
    SELECT
        s.code,
        s.description,
        s.report_month,
        SUM(CASE
            WHEN s.code IN ('1.4.3', '1.5.4', '1.6.9') THEN -ABS(COALESCE(r.total_amount, 0))
            ELSE COALESCE(r.total_amount, 0)
        END) AS amount
    FROM report_skeleton s
    LEFT JOIN raw_data r
        ON s.source_label = r.description
        AND s.report_month = r.report_month
    GROUP BY 1, 2, 3
),

sub_totals AS (
    SELECT
        LEFT(code, 3) AS code,
        CASE
            WHEN code LIKE '1.1.%' THEN 'Liquid Funds'
            WHEN code LIKE '1.4.%' THEN 'Loans, Advances and Discount'
            WHEN code LIKE '1.5.%' THEN 'Accounts Receivable'
            WHEN code LIKE '1.6.%' THEN 'Fixed Assets'
            WHEN code LIKE '1.9.%' THEN 'Other Assets'
        END AS description,
        report_month,
        ROUND(SUM(amount), 2) AS amount
    FROM detailed_report
    GROUP BY 1, 2, 3
),

combined AS (
    SELECT * FROM detailed_report
    UNION ALL
    SELECT * FROM sub_totals
    UNION ALL
    SELECT
        '1.0' AS code,
        'TOTAL ASSETS' AS description,
        report_month,
        ROUND(SUM(amount), 2) AS amount
    FROM sub_totals
    GROUP BY report_month
)

SELECT
    code,
    description,
    report_month,
    CONCAT(
        CASE CAST(SUBSTR(report_month, 6, 2) AS INTEGER)
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
        SUBSTR(report_month, 1, 4)
    ) AS report_month_label,
    amount
FROM combined
ORDER BY report_month DESC, code ASC
