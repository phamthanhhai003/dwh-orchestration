{{ config(
    materialized='table',
    unique_key=['report_month', 'code'],
    incremental_strategy='merge',
    database='hive',
    schema='gold',
    tags=['soc', 'monthly', 'balance_sheet_liabilities']
) }}

WITH report_structure(code, description) AS (
    SELECT * FROM (VALUES
        ('2.1', 'DUE TO CENTRAL BANK'),
        ('2.1.1', 'Loans and Advences'),
        ('2.1.2', 'Discounts'),
        ('2.1.3', 'Others'),
        ('2.2', 'DUE TO COMMERCIAL BANKS'),
        ('2.2.1', 'Local Banks'),
        ('2.2.2', 'Foreign Banks'),
        ('2.2.3', 'Head Office / Parent Bank'),
        ('2.3', 'SECURITIES SOLD UNDER REPO AGREEMENT'),
        ('2.3.1', 'To Central Bank'),
        ('2.3.2', 'To Other Country-Party'),
        ('2.4', 'DEPOSITS'),
        ('2.4.1', 'Resident Domestic Currency'),
        ('2.4.2', 'Non-Resident Domestic Currency'),
        ('2.4.3', 'Resident Foreign Currency'),
        ('2.4.4', 'Non-Resident Foreign Currency'),
        ('2.6', 'OTHER SUNDRY CURRENT LIABILITIES'),
        ('2.6.1', 'Cheques & Other Items in Course of PMT'),
        ('2.6.2', 'Staff Expenses'),
        ('2.6.3', 'Provision for Taxation'),
        ('2.6.4', 'Accounts Payable'),
        ('2.6.5', 'Interest Accrued'),
        ('2.6.6', 'Item in Suspens'),
        ('2.6.7', 'Dividends Payable'),
        ('2.6.8', 'Restricted Deposits'),
        ('2.6.9', 'Unearned Interest'),
        ('2.6.10', 'Sundry Liabilities'),
        ('2.8', 'OTHER LIABILITIES'),
        ('2.8.1', 'Inter Branch Transactin (Net)'),
        ('2.8.2', 'Other'),
        ('3', 'CAPITAL ACCOUNTS'),
        ('3.1', 'Provisioning'),
        ('3.1.1', 'Provision on Loans'),
        ('3.1.2', 'Provision on Securities'),
        ('3.1.3', 'Provision on Exchange Rate'),
        ('3.1.4', 'Other Provision'),
        ('3.2', 'Capital Paid up and Assigned'),
        ('3.2.1', 'Ordinary Shares'),
        ('3.2.2', 'Preference Shares'),
        ('3.2.3', 'Other'),
        ('3.3', 'Share Premium'),
        ('3.4', 'Reserves'),
        ('3.5', 'Retained Earnings / (Accumulated Deficit)'),
        ('3.6', 'Unappropriated Profits / (Losses)'),
        ('3.6.1', 'Previous Financial Year'),
        ('3.6.2', 'Current Financial Year'),
        ('TOTAL_LIAB', 'TOTAL LIABILITIES'),
        ('TOTAL_CAP', 'TOTAL CAPITAL'),
        ('TOTAL_ALL', 'TOTAL LIABILITIES & CAPITAL')
    ) AS t(code, description)
),

aggregated_data AS (
    SELECT
        report_month,
        code,
        description,
        SUM(amount) AS total_amount
    FROM {{ ref('soc_liabilities_real_v2')}}
    WHERE type_period = 'CURR'
    {% if var("target_date", "") != "" %}
    AND report_month = '{{ var("target_date")[0:7] }}'
    {% elif is_incremental() %}
    AND report_month >= (
        SELECT COALESCE(MAX(report_month), '1900-01')
        FROM {{ this }}
    )
    {% endif %}
    GROUP BY report_month, description, code
),

periods AS (
    SELECT DISTINCT report_month FROM aggregated_data
)

SELECT
    s.code,
    s.description,
    p.report_month,
    SUM(CASE
        -- GIỮ NGUYÊN 2.4
        WHEN s.code = '2.4' AND r.description IN ('Demand Deposit', 'Time Deposits', '2.4.1.2.4', 'Resident Foreign Currency', 'Non-Resident Foreign Currency') THEN r.total_amount
        WHEN s.code = '2.4.1' AND r.description IN ('Demand Deposit', 'Time Deposits', '2.4.1.2.4') THEN r.total_amount

        -- SỬA 2.6: Lấy tổng từ các điều kiện của 2.6.x
        WHEN s.code = '2.6' AND (
            r.description = 'Cheques & Other Items in Course of PMT' OR
            r.code = '2.06.2' OR
            r.description IN ('Income Tax', 'Income Tax (5182)', 'Other Taxes') OR
            r.description = 'Collected Tax' OR
            r.description = 'Interest Accrued' OR
            r.description IN ('Items in Suspens', 'Salary Payable', 'Lease Liability IFRS') OR
            (r.code = '2.06.8' OR r.description = 'RESTRICTED DEPOSIT') OR
            (r.code = '2.06.9' OR r.description = 'Unearned Interest') OR
            r.description = 'Sundry Liabilities'
        ) THEN r.total_amount

        -- GIỮ NGUYÊN CÁC MÃ CHI TIẾT 2.6.x
        WHEN s.code = '2.6.1' AND r.description = 'Cheques & Other Items in Course of PMT' THEN r.total_amount
        WHEN s.code = '2.6.2' AND r.code = '2.06.2' THEN r.total_amount
        WHEN s.code = '2.6.3' AND r.description IN ('Income Tax', 'Income Tax (5182)', 'Other Taxes') THEN r.total_amount
        WHEN s.code = '2.6.4' AND r.description = 'Collected Tax' THEN r.total_amount
        WHEN s.code = '2.6.5' AND r.description = 'Interest Accrued' THEN r.total_amount
        WHEN s.code = '2.6.6' AND r.description IN ('Items in Suspens', 'Salary Payable', 'Lease Liability IFRS') THEN r.total_amount
        WHEN s.code = '2.6.8' AND (r.code = '2.06.8' OR r.description = 'RESTRICTED DEPOSIT') THEN r.total_amount
        WHEN s.code = '2.6.9' AND (r.code = '2.06.9' OR r.description = 'Unearned Interest') THEN r.total_amount
        WHEN s.code = '2.6.10' AND r.description = 'Sundry Liabilities' THEN r.total_amount

        -- GIỮ NGUYÊN PHẦN CÒN LẠI
        WHEN s.code = '2.8.1' AND r.description = 'Other Liabilit/Due To Branch' THEN r.total_amount
        WHEN s.code = '2.8.2' AND r.description IN ('Others/Overages', 'Overages ATM') THEN r.total_amount
        WHEN s.code = '2.8' AND r.description IN ('Other Liabilit/Due To Branch', 'Others/Overages', 'Overages ATM') THEN r.total_amount
        WHEN s.code = '3.1' AND r.description = 'Provisioning' THEN r.total_amount
        WHEN s.code IN ('3.2', '3.2.1') AND r.code = '3.02' THEN r.total_amount
        WHEN s.code = '3.4' AND r.description = 'Capital Reserve/Reserve Legais' THEN r.total_amount
        WHEN s.code = '3.6.1' AND r.description = 'Previeous Financial Year' THEN r.total_amount
        WHEN s.code = '3.6.2' AND r.description = 'Current Financial Year' THEN r.total_amount
        WHEN s.code = '3.6' AND r.description IN ('Previeous Financial Year', 'Current Financial Year') THEN r.total_amount

        WHEN s.code = 'TOTAL_LIAB' AND r.description IN ('Demand Deposit', 'Time Deposits', '2.4.1.2.4', 'Resident Foreign Currency', 'Non-Resident Foreign Currency', 'Other Sundry Current Liabilit.', 'Other liabilit/Due To Branch', 'Others/Overages', 'Overages ATM') THEN r.total_amount
        WHEN s.code = 'TOTAL_CAP' AND r.description IN ('Provisioning', 'Capital Paid-up & Assigned', 'Capital Reserve/Reserve Legais', 'Previeous Financial Year', 'Current Financial Year') THEN r.total_amount
        WHEN s.code = 'TOTAL_ALL' AND r.description IN ('Demand Deposit', 'Time Deposits', '2.4.1.2.4', 'Resident Foreign Currency', 'Non-Resident Foreign Currency', 'Other Sundry Current Liabilit.', 'Other liabilit/Due To Branch', 'Others/Overages', 'Overages ATM', 'Provisioning', 'Capital Paid-up & Assigned', 'Capital Reserve/Reserve Legais', 'Previeous Financial Year', 'Current Financial Year') THEN r.total_amount

        ELSE 0
    END) AS amount,
    CONCAT(
        CASE CAST(SUBSTR(p.report_month, 6, 2) AS INTEGER)
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
        SUBSTR(p.report_month, 1, 4)
    ) AS report_month_label
FROM report_structure s
CROSS JOIN periods p
LEFT JOIN aggregated_data r ON p.report_month = r.report_month
GROUP BY p.report_month, s.code, s.description
ORDER BY p.report_month, s.code
