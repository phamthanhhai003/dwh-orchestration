{{ config(
    materialized='table',
    unique_key=['load_date', 'section', 'order_id'],
    incremental_strategy='merge',
    database='hive',
    schema='gold'
) }}

-- Division: sie (Consolidated P&L — Period Comparison, CURRENT PERIOD column only)
-- Strategy: custom multi-source consolidation (no seeds)
-- Source: stg__po_sie_leaves (reused with order_id remapping) + Exp.!Q81 direct
-- Template: 'SIE(consolidated PL).' sheet in docs/report-template/consolidate.xlsx
-- Business doc: docs/business-documentation/sie.md
-- Grain: 1 row per (load_date, section, order_id)
--
-- Sign convention (inherited from stg__po_sie_leaves):
--   - final_income_report  : stored POSITIVE → use as-is
--   - final_expense_report : stored NEGATIVE (T24) → multiply by -1
--
-- Branch filter: branch_name <> 'ATAURO' (Excel consolidates 14 of 15 gold branches).
--
-- Key difference from PO SIE:
--   Row 138 (TOTAL OPERATING EXPENSES) = Exp.!Q81 pull-through from Expense sheet,
--   NOT recomputed from subsection totals. This means the total includes ALL branches
--   for row 107 (5.03.7.6), while the detail row 107 only uses UPGRADE (Excel bug).
--
-- SIE-specific codes (differ from PO SIE):
--   order 48: 4.02.6.1 (PO SIE: 4.02.6)
--   order 67: 5.01.2.3 (PO SIE: 5.01.2.2)
--   order 80: 5.03.1.4 (PO SIE: 5.03.1.1)
--   order 99: 5.03.5.3 (PO SIE: 5.03.5.1)
--
-- Extra row vs PO SIE: order 28 (4.01.4.2 Loan Fees header)
-- Order_id shift: SIE 6-27 ← PO SIE 7-28; SIE 134-141 ← PO SIE 135-142

WITH

-- ── SIE row skeleton (136 rows) ─────────────────────────────────────────────────
row_skeleton AS (
    -- 4.0 INCOME
    SELECT CAST(  6 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.0' AS VARCHAR) AS code, CAST('INCOME :' AS VARCHAR) AS description, CAST('header' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(  7 AS INT), CAST(4 AS INT), CAST('4.01' AS VARCHAR), CAST('INTEREST INCOME' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST(  8 AS INT), CAST(4 AS INT), CAST('4.01.1' AS VARCHAR), CAST('Interest on Due from Banks' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(  9 AS INT), CAST(4 AS INT), CAST('4.01.2' AS VARCHAR), CAST('Interest on Overnight Placement' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 10 AS INT), CAST(4 AS INT), CAST('4.01.3' AS VARCHAR), CAST('Interest on Securities' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 11 AS INT), CAST(4 AS INT), CAST('4.01.4' AS VARCHAR), CAST('Interest & Fees on Loans & Discounts' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 12 AS INT), CAST(4 AS INT), CAST('4.01.4.1' AS VARCHAR), CAST('Interest Income on Loans' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 13 AS INT), CAST(4 AS INT), CAST('4.01.4.1.3.10' AS VARCHAR), CAST('Market Vendor Daily Loans' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 14 AS INT), CAST(4 AS INT), CAST('4.01.4.1.3.20' AS VARCHAR), CAST('Seasonal Crop Loans' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 15 AS INT), CAST(4 AS INT), CAST('4.01.4.1.3.30' AS VARCHAR), CAST('Other Business Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 16 AS INT), CAST(4 AS INT), CAST('4.01.4.1.4.10' AS VARCHAR), CAST('Microfinance Group Loans' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 17 AS INT), CAST(4 AS INT), CAST('4.01.4.1.4.20' AS VARCHAR), CAST('Payroll Loans' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 18 AS INT), CAST(4 AS INT), CAST('4.01.4.1.4.30' AS VARCHAR), CAST('Employee and Staff Loans' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 19 AS INT), CAST(4 AS INT), CAST('4.01.4.1.4.60' AS VARCHAR), CAST('Asuwa''in Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 20 AS INT), CAST(4 AS INT), CAST('4.01.4.1.4.70' AS VARCHAR), CAST('Bukae Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 21 AS INT), CAST(4 AS INT), CAST('4.01.4.1.4.80' AS VARCHAR), CAST('Pension Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 22 AS INT), CAST(4 AS INT), CAST('4.01.4.1.4.40' AS VARCHAR), CAST('Project Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 23 AS INT), CAST(4 AS INT), CAST('4.01.4.1.4.50' AS VARCHAR), CAST('Investment Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 24 AS INT), CAST(4 AS INT), CAST('4.01.4.1.4.85' AS VARCHAR), CAST('Transport Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 25 AS INT), CAST(4 AS INT), CAST('4.01.4.1.4.90' AS VARCHAR), CAST('FIAR Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 26 AS INT), CAST(4 AS INT), CAST('4.01.4.1.5.10' AS VARCHAR), CAST('Agricultor Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 27 AS INT), CAST(4 AS INT), CAST('4.01.4.1.5.11' AS VARCHAR), CAST('RBL Loan Origination' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 28 AS INT), CAST(4 AS INT), CAST('4.01.4.2' AS VARCHAR), CAST('Loan Fees' AS VARCHAR), CAST('header' AS VARCHAR)
    UNION ALL SELECT CAST( 29 AS INT), CAST(4 AS INT), CAST('4.01.4.3' AS VARCHAR), CAST('Loan Fee Income' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 30 AS INT), CAST(4 AS INT), CAST('4.01.4.3.1' AS VARCHAR), CAST('Loan Fee Income Microfin Group' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 31 AS INT), CAST(4 AS INT), CAST('4.01.4.3.3' AS VARCHAR), CAST('Loan Fee Income Market Vendor' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 32 AS INT), CAST(4 AS INT), CAST('4.01.4.3.4' AS VARCHAR), CAST('Loan Fee Income Seasonal Crop Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 33 AS INT), CAST(4 AS INT), CAST('4.01.4.3.5' AS VARCHAR), CAST('Loan Fee Income Business Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 34 AS INT), CAST(4 AS INT), CAST('4.01.4.3.6' AS VARCHAR), CAST('Loan Fee Income Payroll' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 35 AS INT), CAST(4 AS INT), CAST('4.01.4.3.8' AS VARCHAR), CAST('Loan Fee Income Employee and Staff Loans' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 36 AS INT), CAST(4 AS INT), CAST('4.01.4.3.7' AS VARCHAR), CAST('Loan Fee Income Asuwa''in' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 37 AS INT), CAST(4 AS INT), CAST('4.01.4.3.9' AS VARCHAR), CAST('Loan Fee Income Bukae' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 38 AS INT), CAST(4 AS INT), CAST('4.01.4.3.11' AS VARCHAR), CAST('Loan Fee Income Pension Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 39 AS INT), CAST(4 AS INT), CAST('4.01.4.4-0' AS VARCHAR), CAST('Loan Fee Income Project Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 40 AS INT), CAST(4 AS INT), CAST('4-01-4-4-1' AS VARCHAR), CAST('Loan Fee Income Investment Loans' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 41 AS INT), CAST(4 AS INT), CAST('4-01-4-4-2' AS VARCHAR), CAST('Loan Fee Income Transport Loans' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 42 AS INT), CAST(4 AS INT), CAST('4-01-4-4-6' AS VARCHAR), CAST('Loan Fee Income FIAR' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 43 AS INT), CAST(4 AS INT), CAST('4-01-4-4-7' AS VARCHAR), CAST('Loan Fee Income Agricultor' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 44 AS INT), CAST(4 AS INT), CAST('4-01-4-4-8' AS VARCHAR), CAST('Loan Fee Income RBL' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 45 AS INT), CAST(4 AS INT), CAST('4.01.5' AS VARCHAR), CAST('Other Interest Income' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 46 AS INT), CAST(4 AS INT), CAST('4.02' AS VARCHAR), CAST('NON INTEREST INCOME' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 47 AS INT), CAST(4 AS INT), CAST('4.02.5' AS VARCHAR), CAST('Commission Income & Transaction Fees' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 48 AS INT), CAST(4 AS INT), CAST('4.02.6.1' AS VARCHAR), CAST('Other Non Interest Income' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 49 AS INT), CAST(4 AS INT), CAST('4.02.0' AS VARCHAR), CAST('Atm Fee' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 50 AS INT), CAST(4 AS INT), CAST('4.02.2' AS VARCHAR), CAST('MoneyGram Fees' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 51 AS INT), CAST(4 AS INT), CAST('4.02.1' AS VARCHAR), CAST('Foreign Transfer Fees' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 52 AS INT), CAST(4 AS INT), CAST('4.02.4' AS VARCHAR), CAST('Foreign Exchange Gains' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 53 AS INT), CAST(4 AS INT), CAST('4.02.6' AS VARCHAR), CAST('Others' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 54 AS INT), CAST(4 AS INT), CAST('4.02.3' AS VARCHAR), CAST('Collection Loan WO' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 55 AS INT), CAST(4 AS INT), CAST('4.02.7' AS VARCHAR), CAST('Fee From Government' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 56 AS INT), CAST(4 AS INT), CAST('4.02.8' AS VARCHAR), CAST('Fee From Bank Guarante & SI' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 57 AS INT), CAST(4 AS INT), CAST('4.02.9' AS VARCHAR), CAST('Initial Fee, Urgent Fee & OTS Fee' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 58 AS INT), CAST(4 AS INT), CAST('4.03' AS VARCHAR), CAST('EXTRAORDINARY INCOME' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 59 AS INT), CAST(4 AS INT), CAST('4.03.1' AS VARCHAR), CAST('Gains with disposal Of Fixed Asset' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 60 AS INT), CAST(4 AS INT), CAST('4.3.2' AS VARCHAR), CAST('Adjustment for Prior Periods' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 61 AS INT), CAST(4 AS INT), CAST('4.03.3.3' AS VARCHAR), CAST('Miscellaneous Income / Loss' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 62 AS INT), CAST(0 AS INT), CAST(CAST(NULL AS VARCHAR) AS VARCHAR), CAST('TOTAL OPERATING INCOME :' AS VARCHAR), CAST('total' AS VARCHAR)
    -- 5.0 EXPENSE
    UNION ALL SELECT CAST( 63 AS INT), CAST(5 AS INT), CAST('5.0' AS VARCHAR), CAST('EXPENSE :' AS VARCHAR), CAST('header' AS VARCHAR)
    UNION ALL SELECT CAST( 64 AS INT), CAST(5 AS INT), CAST('5.01' AS VARCHAR), CAST('INTEREST EXPENSE' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 65 AS INT), CAST(5 AS INT), CAST('5.01.2' AS VARCHAR), CAST('Interest by Individual Deposits' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 66 AS INT), CAST(5 AS INT), CAST('5.01.2.1' AS VARCHAR), CAST('Saving Deposits' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 67 AS INT), CAST(5 AS INT), CAST('5.01.2.3' AS VARCHAR), CAST('Time Deposits' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 68 AS INT), CAST(5 AS INT), CAST('5.01.2.2' AS VARCHAR), CAST('Demand Deposits' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 69 AS INT), CAST(5 AS INT), CAST('5.01.3' AS VARCHAR), CAST('Interest on Legal Entities Deposits' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 70 AS INT), CAST(5 AS INT), CAST('5.01.3.1' AS VARCHAR), CAST('Saving Deposits' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 71 AS INT), CAST(5 AS INT), CAST('5.01.5' AS VARCHAR), CAST('Interest On Expense On Other Borrowing (Inter Branch)' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 72 AS INT), CAST(5 AS INT), CAST('5.02' AS VARCHAR), CAST('NON INTEREST EXPENSE' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 73 AS INT), CAST(5 AS INT), CAST('5.02.3' AS VARCHAR), CAST('Foreign Exchanges Losses' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 74 AS INT), CAST(5 AS INT), CAST('5.02.4' AS VARCHAR), CAST('Commission Expense & Transaction Fees' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 75 AS INT), CAST(5 AS INT), CAST('5.02.5' AS VARCHAR), CAST('Other Non Interest Expense (License Fee)' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 76 AS INT), CAST(5 AS INT), CAST('5.03' AS VARCHAR), CAST('OPERATING EXPENSE' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 77 AS INT), CAST(5 AS INT), CAST('5.03.1' AS VARCHAR), CAST('Salaries and Employee Benefits' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 78 AS INT), CAST(5 AS INT), CAST('5.03.1.1' AS VARCHAR), CAST('Remunerations of Board of Director' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 79 AS INT), CAST(5 AS INT), CAST('5.03.1.2' AS VARCHAR), CAST('Directors Fees and Honoraria (KF,KNAAR)' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 80 AS INT), CAST(5 AS INT), CAST('5.03.1.4' AS VARCHAR), CAST('Salaries and Wages' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 81 AS INT), CAST(5 AS INT), CAST('5.03.1.3' AS VARCHAR), CAST('Fringe Benefits - Employee' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 82 AS INT), CAST(5 AS INT), CAST('5.03.1.5' AS VARCHAR), CAST('Contribution to Retirement / Prov. Fun' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 83 AS INT), CAST(5 AS INT), CAST('5.03.1.6' AS VARCHAR), CAST('Annual Leave' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 84 AS INT), CAST(5 AS INT), CAST('5.03.1.7' AS VARCHAR), CAST('Payment for Over time' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 85 AS INT), CAST(5 AS INT), CAST('5.03.2' AS VARCHAR), CAST('Administrative Expenses' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 86 AS INT), CAST(5 AS INT), CAST('5.03.2.4' AS VARCHAR), CAST('OTHER ADMINISTRATIVE EXPENSE' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 87 AS INT), CAST(5 AS INT), CAST('5.03.2.4.1.10' AS VARCHAR), CAST('Information Tech./Automation Expenses' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 88 AS INT), CAST(5 AS INT), CAST('5.03.2.4.1.15' AS VARCHAR), CAST('Communication Expense' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 89 AS INT), CAST(5 AS INT), CAST('5.03.2.4.1.20' AS VARCHAR), CAST('Power, Light and Water' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 90 AS INT), CAST(5 AS INT), CAST('5.03.2.4.1.25' AS VARCHAR), CAST('Fuel and Lubricants' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 91 AS INT), CAST(5 AS INT), CAST('5.03.2.4.1.30' AS VARCHAR), CAST('Traveling Expenses' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 92 AS INT), CAST(5 AS INT), CAST('5.03.2.4.1.35' AS VARCHAR), CAST('Stationery and Office Supplies' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 93 AS INT), CAST(5 AS INT), CAST('5.03.2.4.1.40' AS VARCHAR), CAST('Representation and Entertaiment' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 94 AS INT), CAST(5 AS INT), CAST('5.03.2.4.1.45' AS VARCHAR), CAST('Repairs and Maintenance' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 95 AS INT), CAST(5 AS INT), CAST('5.03.3' AS VARCHAR), CAST('Advertising and Public Relation' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 96 AS INT), CAST(5 AS INT), CAST('5.03.4' AS VARCHAR), CAST('Audit,Legal & Professional Fees' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 97 AS INT), CAST(5 AS INT), CAST('5.03.5' AS VARCHAR), CAST('Rents Paid' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 98 AS INT), CAST(5 AS INT), CAST('5.03.5.1' AS VARCHAR), CAST('Rent Paid' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 99 AS INT), CAST(5 AS INT), CAST('5.03.5.3' AS VARCHAR), CAST('Car Rents' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(100 AS INT), CAST(5 AS INT), CAST('5.03.6' AS VARCHAR), CAST('Expense on Premises and Fixed Assets' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST(101 AS INT), CAST(5 AS INT), CAST('5.03.6.1' AS VARCHAR), CAST('Insurance' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(102 AS INT), CAST(5 AS INT), CAST('5.03.6.3' AS VARCHAR), CAST('Building Maintenance' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(103 AS INT), CAST(5 AS INT), CAST('5.03.7' AS VARCHAR), CAST('Depreciation and Amortization' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST(104 AS INT), CAST(5 AS INT), CAST('5.03.7.3' AS VARCHAR), CAST('Depr.Mechinery & Equipment' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(105 AS INT), CAST(5 AS INT), CAST('5.03.7.4' AS VARCHAR), CAST('Depr.Furneture & Fixture' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(106 AS INT), CAST(5 AS INT), CAST('5.03.7.5' AS VARCHAR), CAST('Depr.Vehicles' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(107 AS INT), CAST(5 AS INT), CAST('5.03.7.6' AS VARCHAR), CAST('Depr-Non Physical Assets' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(108 AS INT), CAST(5 AS INT), CAST('5.03.7.7' AS VARCHAR), CAST('Amort.Leasehold Right & Improvement' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(109 AS INT), CAST(5 AS INT), CAST('5.03.7.8' AS VARCHAR), CAST('Amort.Buillding' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(110 AS INT), CAST(5 AS INT), CAST('5.03.7.9' AS VARCHAR), CAST('Amort. Deffered Charges' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(111 AS INT), CAST(5 AS INT), CAST('5.03.8' AS VARCHAR), CAST('Provision' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST(112 AS INT), CAST(5 AS INT), CAST('5.03.8.1' AS VARCHAR), CAST('Provision for loans losses' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(113 AS INT), CAST(5 AS INT), CAST('5.03.9' AS VARCHAR), CAST('Other Operating Expense' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST(114 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.05' AS VARCHAR), CAST('Security,Janitorial & Messengerial Services' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(115 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.55' AS VARCHAR), CAST('Freight and Handling Expenses' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(116 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.60' AS VARCHAR), CAST('Taxes and Licenses' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(117 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.65' AS VARCHAR), CAST('Bad-Debts Written Off' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(118 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.66' AS VARCHAR), CAST('Health Expenses' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(119 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.67' AS VARCHAR), CAST('Miscellaneous Expenses/Transport Expenses' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(120 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.69' AS VARCHAR), CAST('Miscellaneous Expenses/Staff Uniform Exp' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(121 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.70' AS VARCHAR), CAST('Miscellaneous Expenses/Assets' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(122 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.71' AS VARCHAR), CAST('Miscellaneous Expenses/Cleanliness' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(123 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.72' AS VARCHAR), CAST('Miscellaneous Expenses/Kitchenette' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(124 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.74' AS VARCHAR), CAST('Miscellaneous Expenses/Dokumentation' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(125 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.75' AS VARCHAR), CAST('Training Expenses' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(126 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.76' AS VARCHAR), CAST('Miscellaneous Expenses/Dead Contribution' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(127 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.77' AS VARCHAR), CAST('Miscellaneous Expenses/ Wedding Contribution' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(128 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.79' AS VARCHAR), CAST('Miscellaneous Expenses/ Covid 19' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(129 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.81' AS VARCHAR), CAST('Miscellaneous Expenses/ Team Building' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(130 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.82' AS VARCHAR), CAST('Miscellaneous Expenses/ Contingency Expenses' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(131 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.83' AS VARCHAR), CAST('Miscellaneous Esp./Corporate Social Resp. CSR' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(132 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.84' AS VARCHAR), CAST('Miscellaneous Esp./Loan Ext' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(133 AS INT), CAST(5 AS INT), CAST('5.03.9.1.1.78' AS VARCHAR), CAST('Miscellaneous Expenses/Others' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(134 AS INT), CAST(5 AS INT), CAST('5.04' AS VARCHAR), CAST('EXTRAORDINARY EXPENSE' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST(135 AS INT), CAST(5 AS INT), CAST('5.04.1' AS VARCHAR), CAST('Losses With disposal of Fixed Assets' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(136 AS INT), CAST(5 AS INT), CAST('5.04.2' AS VARCHAR), CAST('Adjustment for Prior Periods' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(137 AS INT), CAST(5 AS INT), CAST('5.04.3' AS VARCHAR), CAST('Other Extraordinary Expenses (Contigency)' AS VARCHAR), CAST('leaf' AS VARCHAR)
    -- TOTALS
    UNION ALL SELECT CAST(138 AS INT), CAST(0 AS INT), CAST(CAST(NULL AS VARCHAR) AS VARCHAR), CAST('TOTAL OPERATING EXPENSES :' AS VARCHAR), CAST('total' AS VARCHAR)
    UNION ALL SELECT CAST(139 AS INT), CAST(0 AS INT), CAST(CAST(NULL AS VARCHAR) AS VARCHAR), CAST('NET INCOME (LOSS) BEFORE TAX :' AS VARCHAR), CAST('total' AS VARCHAR)
    UNION ALL SELECT CAST(140 AS INT), CAST(5 AS INT), CAST('5.09' AS VARCHAR), CAST('PROVISION FOR INCOME TAX :' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(141 AS INT), CAST(0 AS INT), CAST(CAST(NULL AS VARCHAR) AS VARCHAR), CAST('NET INCOME (LOSS) AFTER TAX :' AS VARCHAR), CAST('total' AS VARCHAR)
),

-- ── Leaf values from PO SIE staging, remapped to SIE order_ids ──────────────────
-- stg__po_sie_leaves computes the same consolidated leaf values that SIE column C
-- needs (since PO SIE ACTUAL = SIE CURRENT for leaf rows).
-- Order_id remapping: PO SIE 9-28 → SIE 8-27, PO SIE 136-138 → SIE 135-137,
--                     PO SIE 141 → SIE 140. Range 30-133 unchanged.
leaves AS (
    SELECT
        load_date,
        CASE
            WHEN order_id BETWEEN 9 AND 28 THEN order_id - 1
            WHEN order_id BETWEEN 136 AND 138 THEN order_id - 1
            WHEN order_id = 141 THEN 140
            ELSE order_id
        END AS order_id,
        CAST(amount AS DECIMAL(18,2)) AS amount
    FROM {{ ref('stg__po_sie_leaves') }}
    {% if var("target_date", "") != "" %}
    WHERE load_date = DATE '{{ var("target_date") }}'
    {% endif %}
),

-- ── Consolidated 5.03.7.6 across ALL branches (for Exp.!Q81 row 107 correction) ─
-- The leaves CTE has row 107 = UPGRADE-only (Excel Exp.!C49 bug). But Exp.!Q81
-- includes ALL branches for 5.03.7.6. This CTE gets the full consolidated value.
consolidated_107 AS (
    SELECT
        load_date,
        CAST(SUM(CAST(amount AS DECIMAL(18,2)) * CAST(-1 AS INT)) AS DECIMAL(18,2)) AS amount
    FROM {{ ref('final_expense_report') }}
    WHERE branch_name <> 'ATAURO'
      AND code = '5.03.7.6'
      AND description = 'Depr. Non Physical Assets'
    {% if var("target_date", "") != "" %}
      AND load_date = DATE '{{ var("target_date") }}'
    {% endif %}
    GROUP BY load_date
),

-- ── Exp.!Q81: total expense from mapped leaves + row 107 all-branch correction ──
-- SIE row 138 (TOTAL OPERATING EXPENSES) = Exp.!Q81 = the Expense sheet's own
-- consolidated total. This INCLUDES all branches for 5.03.7.6 (unlike leaf row 107
-- which only uses UPGRADE due to the Excel bug).
-- Formula: SUM(expense leaves 66-137) + consolidated_107 - upgrade_107_in_leaves
exp_q81 AS (
    SELECT
        l.load_date,
        CAST(
            SUM(CASE WHEN l.order_id BETWEEN 66 AND 137 THEN l.amount ELSE CAST(0 AS DECIMAL(18,2)) END)
          + COALESCE(MAX(c.amount), CAST(0 AS DECIMAL(18,2)))
          - SUM(CASE WHEN l.order_id = 107 THEN l.amount ELSE CAST(0 AS DECIMAL(18,2)) END)
        AS DECIMAL(18,2)) AS amount
    FROM leaves l
    LEFT JOIN consolidated_107 c ON l.load_date = c.load_date
    GROUP BY l.load_date
),

-- ── Subtotals and totals (computed from leaves + exp_q81) ───────────────────────
subtotals AS (
    -- C12 (4.01.4.1) = SUM(C13:C27) — Interest Income on Loans
    SELECT CAST( 12 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 13 AND 27 GROUP BY load_date
    -- C29 (4.01.4.3) = SUM(C30:C45) — Loan Fee Income (includes C45, always 0)
    UNION ALL SELECT CAST( 29 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 30 AND 45 GROUP BY load_date
    -- C11 (4.01.4) = C12 + C29 = all leaves 13-45
    UNION ALL SELECT CAST( 11 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 13 AND 45 GROUP BY load_date
    -- C7 (4.01) = C8+C9+C10+C11+C45 = all leaves 8-45 (C45=0, double-count is no-op)
    UNION ALL SELECT CAST(  7 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 8 AND 45 GROUP BY load_date
    -- C48 (4.02.6.1) = SUM(C49:C57)
    UNION ALL SELECT CAST( 48 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 49 AND 57 GROUP BY load_date
    -- C46 (4.02) = SUM(C47:C48) = leaves 47 + 49-57
    UNION ALL SELECT CAST( 46 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 47 AND 57 GROUP BY load_date
    -- C58 (4.03) = C59+C60+C61
    UNION ALL SELECT CAST( 58 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 59 AND 61 GROUP BY load_date
    -- C62 (TOTAL OPERATING INCOME) = C7+C46+C58 = all income leaves 8-61
    UNION ALL SELECT CAST( 62 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 8 AND 61 GROUP BY load_date
    -- C65 (5.01.2) = C66+C67+C68
    UNION ALL SELECT CAST( 65 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 66 AND 68 GROUP BY load_date
    -- C69 (5.01.3) = C70
    UNION ALL SELECT CAST( 69 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id = 70 GROUP BY load_date
    -- C64 (5.01) = C65+C69+C71 = leaves 66-71
    UNION ALL SELECT CAST( 64 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 66 AND 71 GROUP BY load_date
    -- C72 (5.02) = C73+C74+C75
    UNION ALL SELECT CAST( 72 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 73 AND 75 GROUP BY load_date
    -- C77 (5.03.1) = SUM(C78:C84)
    UNION ALL SELECT CAST( 77 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 78 AND 84 GROUP BY load_date
    -- C86 (5.03.2.4) = SUM(C87:C94)
    UNION ALL SELECT CAST( 86 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 87 AND 94 GROUP BY load_date
    -- C85 (5.03.2) = C86 (identical)
    UNION ALL SELECT CAST( 85 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 87 AND 94 GROUP BY load_date
    -- C97 (5.03.5) = SUM(C98:C99)
    UNION ALL SELECT CAST( 97 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 98 AND 99 GROUP BY load_date
    -- C100 (5.03.6) = C101+C102
    UNION ALL SELECT CAST(100 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 101 AND 102 GROUP BY load_date
    -- C103 (5.03.7) = SUM(C104:C110)
    UNION ALL SELECT CAST(103 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 104 AND 110 GROUP BY load_date
    -- C111 (5.03.8) = C112
    UNION ALL SELECT CAST(111 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id = 112 GROUP BY load_date
    -- C113 (5.03.9) = SUM(C114:C133)
    UNION ALL SELECT CAST(113 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 114 AND 133 GROUP BY load_date
    -- C76 (5.03) = C77+C85+C97+C95+C96+C100+C103+C111+C113 = all leaves 78-133
    UNION ALL SELECT CAST( 76 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 78 AND 133 GROUP BY load_date
    -- C134 (5.04) = SUM(C135:C137)
    UNION ALL SELECT CAST(134 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 135 AND 137 GROUP BY load_date
    -- C138 (TOTAL OPERATING EXPENSES) = Exp.!Q81 — pull-through from Expense sheet
    UNION ALL SELECT CAST(138 AS INT) AS order_id, load_date, CAST(amount AS DECIMAL(18,2)) AS amount FROM exp_q81
    -- C139 (NET INCOME BEFORE TAX) = C62 - C138
    UNION ALL SELECT CAST(139 AS INT) AS order_id, l.load_date,
        CAST(
            SUM(CASE WHEN l.order_id BETWEEN 8 AND 61 THEN l.amount ELSE CAST(0 AS DECIMAL(18,2)) END)
          - MAX(eq.amount)
        AS DECIMAL(18,2)) AS amount
    FROM leaves l
    JOIN exp_q81 eq ON l.load_date = eq.load_date
    GROUP BY l.load_date
    -- C141 (NET INCOME AFTER TAX) = C139 - C140
    UNION ALL SELECT CAST(141 AS INT) AS order_id, l.load_date,
        CAST(
            SUM(CASE WHEN l.order_id BETWEEN 8 AND 61 THEN l.amount ELSE CAST(0 AS DECIMAL(18,2)) END)
          - MAX(eq.amount)
          - SUM(CASE WHEN l.order_id = 140 THEN l.amount ELSE CAST(0 AS DECIMAL(18,2)) END)
        AS DECIMAL(18,2)) AS amount
    FROM leaves l
    JOIN exp_q81 eq ON l.load_date = eq.load_date
    GROUP BY l.load_date
),

-- ── Combine leaves + subtotals into a single (load_date, order_id, amount) CTE ──
all_amounts AS (
    SELECT load_date, order_id, CAST(amount AS DECIMAL(18,2)) AS amount FROM leaves
    UNION ALL
    SELECT load_date, order_id, CAST(amount AS DECIMAL(18,2)) AS amount FROM subtotals
),

-- ── One load_date per target run (cross-join so every skeleton row has a date) ───
target_dates AS (
    SELECT DISTINCT load_date FROM leaves
),

skeleton_dated AS (
    SELECT
        td.load_date,
        s.order_id,
        s.section,
        s.code,
        s.description,
        s.row_type
    FROM row_skeleton s
    CROSS JOIN target_dates td
)

SELECT
    sd.load_date,
    sd.section,
    sd.order_id,
    sd.code,
    sd.description,
    sd.row_type,
    CAST(COALESCE(a.amount, CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS amount,
    CONCAT(
        CAST(CAST(SUBSTR(sd.load_date, 9, 2) AS INTEGER) AS VARCHAR),
        CASE
            WHEN CAST(SUBSTR(sd.load_date, 9, 2) AS INTEGER) IN (11, 12, 13) THEN 'th'
            WHEN MOD(CAST(SUBSTR(sd.load_date, 9, 2) AS INTEGER), 10) = 1 THEN 'st'
            WHEN MOD(CAST(SUBSTR(sd.load_date, 9, 2) AS INTEGER), 10) = 2 THEN 'nd'
            WHEN MOD(CAST(SUBSTR(sd.load_date, 9, 2) AS INTEGER), 10) = 3 THEN 'rd'
            ELSE 'th'
        END,
        ' ',
        CASE CAST(SUBSTR(sd.load_date, 6, 2) AS INTEGER)
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
        SUBSTR(sd.load_date, 1, 4)
    ) AS load_date_label,
    CURRENT_TIMESTAMP                                                    AS updated_at
FROM skeleton_dated sd
LEFT JOIN all_amounts a
       ON sd.load_date = a.load_date
      AND sd.order_id  = a.order_id

