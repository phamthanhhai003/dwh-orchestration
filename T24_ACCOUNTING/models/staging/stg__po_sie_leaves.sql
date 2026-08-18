{{ config(
    materialized='table',
    unique_key=['load_date', 'order_id'],
    incremental_strategy='merge',
    database='hive',
    schema='silver',
    tags=['po_sie', 'staging']
) }}

-- Staging: PO SIE leaf values
-- Computes the 96 leaf lookups by joining gold sources against a static
-- leaf_map (code, description → order_id). Row 107 (5.03.7.6) is handled
-- separately to replicate the Excel =Exp.!C49 single-branch (UPGRADE) bug.
--
-- Sign convention: income POSITIVE; expense flipped by * -1 (T24).
-- Branch filter: branch_name <> 'ATAURO' (consolidation covers 14 branches).
--
-- Persisted so the final report model avoids re-inlining leaves 25 times
-- across its subtotal computations.

WITH
income_src AS (
    SELECT
        CAST('INC' AS VARCHAR)              AS src_tag,
        load_date,
        CAST(code        AS VARCHAR)        AS code,
        CAST(description AS VARCHAR)        AS description,
        CAST(amount      AS DECIMAL(18,2))  AS amount
    FROM {{ ref('final_income_report') }}
    WHERE branch_name <> 'ATAURO'
    {% if var("target_date", "") != "" %}
      AND load_date = DATE '{{ var("target_date") }}'
    {% endif %}
),

expense_src AS (
    SELECT
        CAST('EXP' AS VARCHAR)                   AS src_tag,
        load_date,
        CAST(code        AS VARCHAR)             AS code,
        CAST(description AS VARCHAR)             AS description,
        CAST(amount      AS DECIMAL(18,2)) * -1  AS amount
    FROM {{ ref('final_expense_report') }}
    WHERE branch_name <> 'ATAURO'
    {% if var("target_date", "") != "" %}
      AND load_date = DATE '{{ var("target_date") }}'
    {% endif %}
),

all_src AS (
    SELECT src_tag, load_date, code, description, amount FROM income_src
    UNION ALL
    SELECT src_tag, load_date, code, description, amount FROM expense_src
),

-- Leaf lookup map: (src_tag, code, description) → order_id
-- Excludes row 107 (handled in leaves_107 due to UPGRADE-only bug).
leaf_map AS (
    SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.1' AS VARCHAR) AS code, CAST('Interest on Due from Banks' AS VARCHAR) AS description, CAST(  9 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.2' AS VARCHAR) AS code, CAST('Interest on Overnight Placement' AS VARCHAR) AS description, CAST( 10 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.3' AS VARCHAR) AS code, CAST('Interest On Securities' AS VARCHAR) AS description, CAST( 11 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.3.10' AS VARCHAR) AS code, CAST('Market Vendor Daily Loans' AS VARCHAR) AS description, CAST( 14 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.3.20' AS VARCHAR) AS code, CAST('Seasonal Crop Loans' AS VARCHAR) AS description, CAST( 15 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.3.30' AS VARCHAR) AS code, CAST('Other Business Loan' AS VARCHAR) AS description, CAST( 16 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.4.10' AS VARCHAR) AS code, CAST('Microfinance Group Loans' AS VARCHAR) AS description, CAST( 17 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.4.20' AS VARCHAR) AS code, CAST('Payroll Loans' AS VARCHAR) AS description, CAST( 18 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.4.86' AS VARCHAR) AS code, CAST('Payroll Loan LO' AS VARCHAR) AS description, CAST( 18 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.4.30' AS VARCHAR) AS code, CAST('Employee and Staff Loans' AS VARCHAR) AS description, CAST( 19 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.4.35' AS VARCHAR) AS code, CAST('Internal Staff Loans' AS VARCHAR) AS description, CAST( 19 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.4.40' AS VARCHAR) AS code, CAST('Asuwa''in Loan' AS VARCHAR) AS description, CAST( 20 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.4.87' AS VARCHAR) AS code, CAST('Asu''wain Loan LO' AS VARCHAR) AS description, CAST( 20 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.4.70' AS VARCHAR) AS code, CAST('Bukae Loan' AS VARCHAR) AS description, CAST( 21 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.4.88' AS VARCHAR) AS code, CAST('Bukae Loan LO' AS VARCHAR) AS description, CAST( 21 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.4.80' AS VARCHAR) AS code, CAST('Pension Loan' AS VARCHAR) AS description, CAST( 22 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.4.89' AS VARCHAR) AS code, CAST('Pension Loan LO' AS VARCHAR) AS description, CAST( 22 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.4.50' AS VARCHAR) AS code, CAST('Project Loan' AS VARCHAR) AS description, CAST( 23 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.4.60' AS VARCHAR) AS code, CAST('Investment Loan' AS VARCHAR) AS description, CAST( 24 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.4.85' AS VARCHAR) AS code, CAST('Transport Loan' AS VARCHAR) AS description, CAST( 25 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.4.90' AS VARCHAR) AS code, CAST('FIAR Loan' AS VARCHAR) AS description, CAST( 26 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.1.5.10' AS VARCHAR) AS code, CAST('Agricultor Loan' AS VARCHAR) AS description, CAST( 27 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST(CAST(NULL AS VARCHAR) AS VARCHAR) AS code, CAST('RBL Loan Origination' AS VARCHAR) AS description, CAST( 28 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.3.1' AS VARCHAR) AS code, CAST('Loan Fee Income Microfin Group' AS VARCHAR) AS description, CAST( 30 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.3.4' AS VARCHAR) AS code, CAST('Loan Fee Income Seasonal Crop Loan' AS VARCHAR) AS description, CAST( 32 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.3.5' AS VARCHAR) AS code, CAST('Loan Fee Income Business Loan' AS VARCHAR) AS description, CAST( 33 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.3.6' AS VARCHAR) AS code, CAST('Loan Fee Income Payroll' AS VARCHAR) AS description, CAST( 34 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4-01-4-4-3' AS VARCHAR) AS code, CAST('Loan Fee Income Payroll Loans LO' AS VARCHAR) AS description, CAST( 34 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.3.7' AS VARCHAR) AS code, CAST('Loan Fee Income Employee and Staff Loans' AS VARCHAR) AS description, CAST( 35 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.3.8' AS VARCHAR) AS code, CAST('Loan Fee Income Internal Staff Loans' AS VARCHAR) AS description, CAST( 35 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.3.9' AS VARCHAR) AS code, CAST('Loan Fee Income Asuwa''in' AS VARCHAR) AS description, CAST( 36 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4-01-4-4-4' AS VARCHAR) AS code, CAST('Loan Fee Income Asuwain Loans LO' AS VARCHAR) AS description, CAST( 36 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.3.10' AS VARCHAR) AS code, CAST('Loan Fee Income Bukae Loan' AS VARCHAR) AS description, CAST( 37 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4-01-4-4-5' AS VARCHAR) AS code, CAST('Loan Fee Income Bukae Loans LO' AS VARCHAR) AS description, CAST( 37 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.3.11' AS VARCHAR) AS code, CAST('Loan Fee Income Pension Loan' AS VARCHAR) AS description, CAST( 38 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01-5' AS VARCHAR) AS code, CAST('Loan Fee Inc Pension L.O' AS VARCHAR) AS description, CAST( 38 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.01.4.4-0' AS VARCHAR) AS code, CAST('Loan Fee Income Project Loan' AS VARCHAR) AS description, CAST( 39 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4-01-4-4-1' AS VARCHAR) AS code, CAST('Loan Fee Income Investment Loans' AS VARCHAR) AS description, CAST( 40 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4-01-4-4-2' AS VARCHAR) AS code, CAST('Loan Fee Income Transport Loans' AS VARCHAR) AS description, CAST( 41 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4-01-4-4-6' AS VARCHAR) AS code, CAST('Loan Fee Income FIAR' AS VARCHAR) AS description, CAST( 42 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4-01-4-4-7' AS VARCHAR) AS code, CAST('Loan Fee Income Agricultor' AS VARCHAR) AS description, CAST( 43 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST(CAST(NULL AS VARCHAR) AS VARCHAR) AS code, CAST('Loan Fee Income RBL' AS VARCHAR) AS description, CAST( 44 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.02.5' AS VARCHAR) AS code, CAST('Commission Income & Transaction Fees' AS VARCHAR) AS description, CAST( 47 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.02.0' AS VARCHAR) AS code, CAST('ATM Fee' AS VARCHAR) AS description, CAST( 49 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.02.2' AS VARCHAR) AS code, CAST('Money Gram Fees' AS VARCHAR) AS description, CAST( 50 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.02.1' AS VARCHAR) AS code, CAST('Foreign Transfer Fees' AS VARCHAR) AS description, CAST( 51 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.02.4' AS VARCHAR) AS code, CAST('Foreign Exchange Gains' AS VARCHAR) AS description, CAST( 52 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.02.6' AS VARCHAR) AS code, CAST('Other Non Interest Income' AS VARCHAR) AS description, CAST( 53 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.02.3' AS VARCHAR) AS code, CAST('Collection Loan Write Off' AS VARCHAR) AS description, CAST( 54 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.02.7' AS VARCHAR) AS code, CAST('Fee From Government' AS VARCHAR) AS description, CAST( 55 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.02.8' AS VARCHAR) AS code, CAST('Fee From Bank Guarante SI Fee' AS VARCHAR) AS description, CAST( 56 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.02.9' AS VARCHAR) AS code, CAST('Initial Fee Urgent Fee OTS Fee' AS VARCHAR) AS description, CAST( 57 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.03.1' AS VARCHAR) AS code, CAST('Gains with disposal Of Fixed Asset' AS VARCHAR) AS description, CAST( 59 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.3.2' AS VARCHAR) AS code, CAST('Adjustment for Prior Periods' AS VARCHAR) AS description, CAST( 60 AS INT) AS order_id
    UNION ALL SELECT CAST('INC' AS VARCHAR) AS src_tag, CAST('4.03.3.3' AS VARCHAR) AS code, CAST('Miscellaneous Income / Loss' AS VARCHAR) AS description, CAST( 61 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.01.2.1' AS VARCHAR) AS code, CAST('Saving Deposits' AS VARCHAR) AS description, CAST( 66 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.01.2.2' AS VARCHAR) AS code, CAST('Time Deposits' AS VARCHAR) AS description, CAST( 67 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.01.2.2' AS VARCHAR) AS code, CAST('Demand Deposits' AS VARCHAR) AS description, CAST( 68 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.01.3.1' AS VARCHAR) AS code, CAST('Saving Deposits' AS VARCHAR) AS description, CAST( 70 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.01.5' AS VARCHAR) AS code, CAST('INT. EXP. ON OTHER BORROWINGS' AS VARCHAR) AS description, CAST( 71 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.02.3' AS VARCHAR) AS code, CAST('Foreign Exchanges Losses' AS VARCHAR) AS description, CAST( 73 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.02.4' AS VARCHAR) AS code, CAST('Commission Expense & Transaction Fees' AS VARCHAR) AS description, CAST( 74 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.02.5' AS VARCHAR) AS code, CAST('Other Non Interest Expense' AS VARCHAR) AS description, CAST( 75 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.1.1' AS VARCHAR) AS code, CAST('Remunerations of Board of Director' AS VARCHAR) AS description, CAST( 78 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.1.2' AS VARCHAR) AS code, CAST('Directors Fees and Honoraria' AS VARCHAR) AS description, CAST( 79 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.1.1' AS VARCHAR) AS code, CAST('Salaries and Wages' AS VARCHAR) AS description, CAST( 80 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.1.3' AS VARCHAR) AS code, CAST('Fringe Benefits - Employee' AS VARCHAR) AS description, CAST( 81 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.1.5' AS VARCHAR) AS code, CAST('Contribution to Retirement' AS VARCHAR) AS description, CAST( 82 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.1.6' AS VARCHAR) AS code, CAST('Annual Leave' AS VARCHAR) AS description, CAST( 83 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.1.7' AS VARCHAR) AS code, CAST('Payment for Over time' AS VARCHAR) AS description, CAST( 84 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.2.4.1.10' AS VARCHAR) AS code, CAST('Information Tech./Automation Expense' AS VARCHAR) AS description, CAST( 87 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.2.4.1.15' AS VARCHAR) AS code, CAST('Communication Expense' AS VARCHAR) AS description, CAST( 88 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.2.4.1.20' AS VARCHAR) AS code, CAST('Power, Light and Water' AS VARCHAR) AS description, CAST( 89 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.2.4.1.25' AS VARCHAR) AS code, CAST('Fuel and Lubricants' AS VARCHAR) AS description, CAST( 90 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.2.4.1.30' AS VARCHAR) AS code, CAST('Traveling Expenses' AS VARCHAR) AS description, CAST( 91 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.2.4.1.35' AS VARCHAR) AS code, CAST('Stationery and Office Supplies' AS VARCHAR) AS description, CAST( 92 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.2.4.1.40' AS VARCHAR) AS code, CAST('Representation and Entertainment' AS VARCHAR) AS description, CAST( 93 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.2.4.1.45' AS VARCHAR) AS code, CAST('Repairs and Maintenance' AS VARCHAR) AS description, CAST( 94 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.3' AS VARCHAR) AS code, CAST('Advertising and Public Relation' AS VARCHAR) AS description, CAST( 95 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.4' AS VARCHAR) AS code, CAST('Audit, Legal & Professional Fees' AS VARCHAR) AS description, CAST( 96 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.5' AS VARCHAR) AS code, CAST('Rents Paid' AS VARCHAR) AS description, CAST( 98 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.5.1' AS VARCHAR) AS code, CAST('Car Rents' AS VARCHAR) AS description, CAST( 99 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.6.1' AS VARCHAR) AS code, CAST('Insurance' AS VARCHAR) AS description, CAST(101 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.6.3' AS VARCHAR) AS code, CAST('Building Maintenance' AS VARCHAR) AS description, CAST(102 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.7.3' AS VARCHAR) AS code, CAST('Depr. Machinery & Equipment' AS VARCHAR) AS description, CAST(104 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.7.4' AS VARCHAR) AS code, CAST('Depr. Furniture, Fixture & Equipment' AS VARCHAR) AS description, CAST(105 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.7.5' AS VARCHAR) AS code, CAST('Depr. Vehicles' AS VARCHAR) AS description, CAST(106 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.7.7' AS VARCHAR) AS code, CAST('Amort. Leasehold Right & Improvement' AS VARCHAR) AS description, CAST(108 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.7.8' AS VARCHAR) AS code, CAST('Amort. Building' AS VARCHAR) AS description, CAST(109 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST(CAST(NULL AS VARCHAR) AS VARCHAR) AS code, CAST('Amort. Deferred Charges' AS VARCHAR) AS description, CAST(110 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.8.1' AS VARCHAR) AS code, CAST('Provision for loans losses' AS VARCHAR) AS description, CAST(112 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.9.1.1.05' AS VARCHAR) AS code, CAST('Security, Janitorial & Messengerial' AS VARCHAR) AS description, CAST(114 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.9.1.1.55' AS VARCHAR) AS code, CAST('Freight and Handling Expenses' AS VARCHAR) AS description, CAST(115 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.9.1.1.60' AS VARCHAR) AS code, CAST('Taxes and Licenses' AS VARCHAR) AS description, CAST(116 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.9.1.1.65' AS VARCHAR) AS code, CAST('Bad-Debts Written Off' AS VARCHAR) AS description, CAST(117 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.9.1.1.66' AS VARCHAR) AS code, CAST('Health Expenses' AS VARCHAR) AS description, CAST(118 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.9.1.1.67' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/Transport' AS VARCHAR) AS description, CAST(119 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.9.1.1.69' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/Staff Uniform' AS VARCHAR) AS description, CAST(120 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.9.1.1.70' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/ Assets' AS VARCHAR) AS description, CAST(121 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.9.1.1.84' AS VARCHAR) AS code, CAST('Miscellaneous Exp/Loan Ext' AS VARCHAR) AS description, CAST(132 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.03.9.1.1.78' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/Others' AS VARCHAR) AS description, CAST(133 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.04.1' AS VARCHAR) AS code, CAST('Losses With disposal of Fixed Assets' AS VARCHAR) AS description, CAST(136 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.04.2' AS VARCHAR) AS code, CAST('Adjustment for Prior Periods' AS VARCHAR) AS description, CAST(137 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.04.3' AS VARCHAR) AS code, CAST('Other Extraordinary Expenses' AS VARCHAR) AS description, CAST(138 AS INT) AS order_id
    UNION ALL SELECT CAST('EXP' AS VARCHAR) AS src_tag, CAST('5.09' AS VARCHAR) AS code, CAST('PROVISION FOR INCOME TAX' AS VARCHAR) AS description, CAST(141 AS INT) AS order_id
),

-- Leaves via single NULL-safe JOIN (handles NULL-code rows 28, 44, 110).
leaves_joined AS (
    SELECT
        src.load_date,
        m.order_id,
        SUM(src.amount) AS amount
    FROM all_src src
    JOIN leaf_map m
      ON src.src_tag = m.src_tag
     AND src.description = m.description
     AND (src.code = m.code OR (src.code IS NULL AND m.code IS NULL))
    GROUP BY src.load_date, m.order_id
),

-- Row 107 Excel bug: =Exp.!C49 (Head Office / UPGRADE only, not consolidated).
leaves_107 AS (
    SELECT
        load_date,
        CAST(107 AS INT) AS order_id,
        SUM(CAST(amount AS DECIMAL(18,2)) * -1) AS amount
    FROM {{ ref('final_expense_report') }}
    WHERE branch_name = 'UPGRADE'
      AND code = '5.03.7.6'
      AND description = 'Depr. Non Physical Assets'
    {% if var("target_date", "") != "" %}
      AND load_date = DATE '{{ var("target_date") }}'
    {% endif %}
    GROUP BY load_date
)

SELECT load_date, order_id, CAST(amount AS DECIMAL(18,2)) AS amount, CURRENT_TIMESTAMP AS updated_at
FROM leaves_joined
UNION ALL
SELECT load_date, order_id, CAST(amount AS DECIMAL(18,2)) AS amount, CURRENT_TIMESTAMP AS updated_at
FROM leaves_107

