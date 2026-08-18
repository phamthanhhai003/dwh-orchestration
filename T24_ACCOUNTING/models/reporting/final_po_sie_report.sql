{{ config(
    materialized='table',
    unique_key=['load_date', 'section', 'order_id'],
    incremental_strategy='merge',
    database='hive',
    schema='gold'
) }}

-- Division: po_sie (Consolidated P&L — Plan vs Actual, ACTUAL column only)
-- Strategy: custom multi-source consolidation (no seeds)
-- Source: hive.gold.final_income_report + hive.gold.final_expense_report
-- Template: 'PO SIE (consolidated PL).' sheet in docs/report-template/consolidate.xlsx
-- Business doc: docs/business-documentation/po_sie.md
-- Grain: 1 row per (load_date, section, order_id)
--
-- Sign convention:
--   - final_income_report  : stored POSITIVE → use as-is
--   - final_expense_report : stored NEGATIVE (T24) → multiply by -1
--
-- Branch filter: branch_name <> 'ATAURO' (Excel consolidates 14 of 15 gold branches).
--
-- Row 107 (5.03.7.6) replicates an Excel bug: formula is =Exp.!C49 (Head Office
-- column only) instead of =Exp.!Q49 (consolidated). Implemented via a dedicated
-- UPGRADE-only lookup CTE.
--
-- Hardcoded-zero rows (no gold lookup, value = 0):
--   order 31  (4.01.4.3.3 Loan Fee Income Market Vendor) — SIE.C31 literal 0
--   order 45  (4.01.5 Other Interest Income)             — SIE.C45 empty
--   order 128 (5.03.9.1.1.79 Covid 19)                   — Exp.!Q71 empty
-- Not-in-gold rows (output 0.00):
--   orders 122, 123, 124, 125, 126, 127, 129, 130, 131
-- NULL-code rows (match by description only):
--   order  28 (RBL Loan Origination)
--   order  44 (Loan Fee Income RBL)
--   order 110 (Amort. Deferred Charges)

WITH

-- ── PO SIE row skeleton (135 rows) ─────────────────────────────────────────────
row_skeleton AS (
    SELECT CAST(  7 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.0' AS VARCHAR) AS code, CAST('INCOME :' AS VARCHAR) AS description, CAST('header' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(  8 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01' AS VARCHAR) AS code, CAST('INTEREST INCOME' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(  9 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.1' AS VARCHAR) AS code, CAST('Interest on Due from Banks' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 10 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.2' AS VARCHAR) AS code, CAST('Interest on Overnight Placement' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 11 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.3' AS VARCHAR) AS code, CAST('Interest on Securities' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 12 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4' AS VARCHAR) AS code, CAST('Interest & Fees on Loans & Discounts' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 13 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1' AS VARCHAR) AS code, CAST('Interest Income on Loans' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 14 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1.3.10' AS VARCHAR) AS code, CAST('Market Vendor Daily Loans' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 15 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1.3.20' AS VARCHAR) AS code, CAST('Seasonal Crop Loans' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 16 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1.3.30' AS VARCHAR) AS code, CAST('Other Business Loan' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 17 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1.4.10' AS VARCHAR) AS code, CAST('Microfinance Group Loans' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 18 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1.4.20' AS VARCHAR) AS code, CAST('Payroll Loans' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 19 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1.4.30' AS VARCHAR) AS code, CAST('Employee and Staff Loans' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 20 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1.4.60' AS VARCHAR) AS code, CAST('Asuwa''in Loan' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 21 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1.4.70' AS VARCHAR) AS code, CAST('Bukae Loan' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 22 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1.4.80' AS VARCHAR) AS code, CAST('Pension Loan' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 23 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1.4.40' AS VARCHAR) AS code, CAST('Project Loan' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 24 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1.4.50' AS VARCHAR) AS code, CAST('Investment Loan' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 25 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1.4.85' AS VARCHAR) AS code, CAST('Transport Loan' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 26 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1.4.90' AS VARCHAR) AS code, CAST('FIAR Loan' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 27 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1.5.10' AS VARCHAR) AS code, CAST('Agricultor Loan' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 28 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.1.5.11' AS VARCHAR) AS code, CAST('RBL Loan Origination' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 29 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.3' AS VARCHAR) AS code, CAST('Loan Fee Income' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 30 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.3.1' AS VARCHAR) AS code, CAST('Loan Fee Income Microfin Group' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 31 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.3.3' AS VARCHAR) AS code, CAST('Loan Fee Income Market Vendor' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 32 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.3.4' AS VARCHAR) AS code, CAST('Loan Fee Income Seasonal Crop Loan' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 33 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.3.5' AS VARCHAR) AS code, CAST('Loan Fee Income Business Loan' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 34 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.3.6' AS VARCHAR) AS code, CAST('Loan Fee Income Payroll' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 35 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.3.8' AS VARCHAR) AS code, CAST('Loan Fee Income Employee and Staff Loans' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 36 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.3.7' AS VARCHAR) AS code, CAST('Loan Fee Income Asuwa''in' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 37 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.3.9' AS VARCHAR) AS code, CAST('Loan Fee Income Bukae' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 38 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.3.11' AS VARCHAR) AS code, CAST('Loan Fee Income Pension Loan' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 39 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.4.4-0' AS VARCHAR) AS code, CAST('Loan Fee Income Project Loan' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 40 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4-01-4-4-1' AS VARCHAR) AS code, CAST('Loan Fee Income Investment Loans' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 41 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4-01-4-4-2' AS VARCHAR) AS code, CAST('Loan Fee Income Transport Loans' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 42 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4-01-4-4-6' AS VARCHAR) AS code, CAST('Loan Fee Income FIAR' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 43 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4-01-4-4-7' AS VARCHAR) AS code, CAST('Loan Fee Income Agricultor' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 44 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4-01-4-4-8' AS VARCHAR) AS code, CAST('Loan Fee Income RBL' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 45 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.01.5' AS VARCHAR) AS code, CAST('Other Interest Income' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 46 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.02' AS VARCHAR) AS code, CAST('NON INTEREST INCOME' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 47 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.02.5' AS VARCHAR) AS code, CAST('Commission Income & Transaction Fees' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 48 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.02.6' AS VARCHAR) AS code, CAST('Other Non Interest Income' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 49 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.02.0' AS VARCHAR) AS code, CAST('ATM Fee' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 50 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.02.2' AS VARCHAR) AS code, CAST('MoneyGram Fees' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 51 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.02.1' AS VARCHAR) AS code, CAST('Foreign Transfer Fees' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 52 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.02.4' AS VARCHAR) AS code, CAST('Foreign Exchange Gains' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 53 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.02.6' AS VARCHAR) AS code, CAST('Others' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 54 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.02.3' AS VARCHAR) AS code, CAST('Collection Loan WO' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 55 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.02.7' AS VARCHAR) AS code, CAST('Fee From Government' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 56 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.02.8' AS VARCHAR) AS code, CAST('Fee From Bank Guarante & SI' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 57 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.02.9' AS VARCHAR) AS code, CAST('Initial Fee, Urgent Fee & OTS Fee' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 58 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.03' AS VARCHAR) AS code, CAST('EXTRAORDINARY INCOME' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 59 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.03.1' AS VARCHAR) AS code, CAST('Gains with disposal Of Fixed Asset' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 60 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.3.2' AS VARCHAR) AS code, CAST('Adjustment for Prior Periods' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 61 AS INT) AS order_id, CAST(4 AS INT) AS section, CAST('4.03.3.3' AS VARCHAR) AS code, CAST('Miscellaneous Income / Loss' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 62 AS INT) AS order_id, CAST(0 AS INT) AS section, CAST(CAST(NULL AS VARCHAR) AS VARCHAR) AS code, CAST('TOTAL OPERATING INCOME' AS VARCHAR) AS description, CAST('total' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 63 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.0' AS VARCHAR) AS code, CAST('EXPENSE :' AS VARCHAR) AS description, CAST('header' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 64 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.01' AS VARCHAR) AS code, CAST('INTEREST EXPENSE' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 65 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.01.2' AS VARCHAR) AS code, CAST('Interest by Individual Deposits' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 66 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.01.2.1' AS VARCHAR) AS code, CAST('Saving Deposits' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 67 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.01.2.2' AS VARCHAR) AS code, CAST('Time Deposits' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 68 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.01.2.2' AS VARCHAR) AS code, CAST('Demand Deposits' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 69 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.01.3' AS VARCHAR) AS code, CAST('Interest on Legal Entities Deposits' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 70 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.01.3.1' AS VARCHAR) AS code, CAST('Saving Deposits (Legal)' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 71 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.01.5' AS VARCHAR) AS code, CAST('Interest On Expense On Other Borrowing (Inter Branch)' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 72 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.02' AS VARCHAR) AS code, CAST('NON INTEREST EXPENSE' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 73 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.02.3' AS VARCHAR) AS code, CAST('Foreign Exchanges Losses' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 74 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.02.4' AS VARCHAR) AS code, CAST('Commission Expense & Transaction Fees' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 75 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.02.5' AS VARCHAR) AS code, CAST('Other Non Interest Expense (License Fee)' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 76 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03' AS VARCHAR) AS code, CAST('OPERATING EXPENSE' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 77 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.1' AS VARCHAR) AS code, CAST('Salaries and Employee Benefits' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 78 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.1.1' AS VARCHAR) AS code, CAST('Remunerations of Board of Director' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 79 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.1.2' AS VARCHAR) AS code, CAST('Directors Fees and Honoraria (KF,KNAAR)' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 80 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.1.1' AS VARCHAR) AS code, CAST('Salaries and Wages' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 81 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.1.3' AS VARCHAR) AS code, CAST('Fringe Benefits - Employee' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 82 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.1.5' AS VARCHAR) AS code, CAST('Contribution to Retirement / Prov. Fun' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 83 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.1.6' AS VARCHAR) AS code, CAST('Annual Leave' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 84 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.1.7' AS VARCHAR) AS code, CAST('Payment for Over time' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 85 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.2' AS VARCHAR) AS code, CAST('Administrative Expenses' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 86 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.2.4' AS VARCHAR) AS code, CAST('OTHER ADMINISTRATIVE EXPENSE' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 87 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.2.4.1.10' AS VARCHAR) AS code, CAST('Information Tech./Automation Expenses' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 88 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.2.4.1.15' AS VARCHAR) AS code, CAST('Communication Expense' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 89 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.2.4.1.20' AS VARCHAR) AS code, CAST('Power, Light and Water' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 90 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.2.4.1.25' AS VARCHAR) AS code, CAST('Fuel and Lubricants' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 91 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.2.4.1.30' AS VARCHAR) AS code, CAST('Traveling Expenses' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 92 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.2.4.1.35' AS VARCHAR) AS code, CAST('Stationery and Office Supplies' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 93 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.2.4.1.40' AS VARCHAR) AS code, CAST('Representation and Entertainment' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 94 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.2.4.1.45' AS VARCHAR) AS code, CAST('Repairs and Maintenance' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 95 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.3' AS VARCHAR) AS code, CAST('Advertising and Public Relation' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 96 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.4' AS VARCHAR) AS code, CAST('Audit,Legal & Professional Fees' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 97 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.5' AS VARCHAR) AS code, CAST('Rents Paid' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 98 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.5.1' AS VARCHAR) AS code, CAST('Rent Paid' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST( 99 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.5.1' AS VARCHAR) AS code, CAST('Car Rents' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(100 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.6' AS VARCHAR) AS code, CAST('Expense on Premises and Fixed Assets' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(101 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.6.1' AS VARCHAR) AS code, CAST('Insurance' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(102 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.6.3' AS VARCHAR) AS code, CAST('Building Maintenance' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(103 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.7' AS VARCHAR) AS code, CAST('Depreciation and Amortization' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(104 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.7.3' AS VARCHAR) AS code, CAST('Depr.Mechinery & Equipment' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(105 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.7.4' AS VARCHAR) AS code, CAST('Depr.Furneture & Fixture' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(106 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.7.5' AS VARCHAR) AS code, CAST('Depr.Vehicles' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(107 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.7.6' AS VARCHAR) AS code, CAST('Depr-Non Physical Assets' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(108 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.7.7' AS VARCHAR) AS code, CAST('Amort.Leasehold Right & Improvement' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(109 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.7.8' AS VARCHAR) AS code, CAST('Amort.Buillding' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(110 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.7.9' AS VARCHAR) AS code, CAST('Amort. Deffered Charges' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(111 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.8' AS VARCHAR) AS code, CAST('Provision' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(112 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.8.1' AS VARCHAR) AS code, CAST('Provision for loans losses' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(113 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9' AS VARCHAR) AS code, CAST('Other Operating Expense' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(114 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.05' AS VARCHAR) AS code, CAST('Security,Janitorial & Messengerial Services' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(115 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.55' AS VARCHAR) AS code, CAST('Freight and Handling Expenses' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(116 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.60' AS VARCHAR) AS code, CAST('Taxes and Licenses' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(117 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.65' AS VARCHAR) AS code, CAST('Bad-Debts Written Off' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(118 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.66' AS VARCHAR) AS code, CAST('Health Expenses' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(119 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.67' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/Transport Expenses' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(120 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.69' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/Staff Uniform Exp' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(121 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.70' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/Assets' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(122 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.71' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/Cleanliness' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(123 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.72' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/Kitchenette' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(124 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.74' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/Dokumentation' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(125 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.75' AS VARCHAR) AS code, CAST('Training Expenses' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(126 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.76' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/Dead Contribution' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(127 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.77' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/ Wedding Contribution' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(128 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.79' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/ Covid 19' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(129 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.81' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/ Team Building' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(130 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.82' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/ Contingency Expenses' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(131 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.83' AS VARCHAR) AS code, CAST('Miscellaneous Esp./Corporate Social Resp. CSR' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(132 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.84' AS VARCHAR) AS code, CAST('Miscellaneous Esp./Loan Ext' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(133 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.03.9.1.1.78' AS VARCHAR) AS code, CAST('Miscellaneous Expenses/Others' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(135 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.04' AS VARCHAR) AS code, CAST('EXTRAORDINARY EXPENSE' AS VARCHAR) AS description, CAST('subtotal' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(136 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.04.1' AS VARCHAR) AS code, CAST('Losses With disposal of Fixed Assets' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(137 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.04.2' AS VARCHAR) AS code, CAST('Adjustment for Prior Periods' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(138 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.04.3' AS VARCHAR) AS code, CAST('Other Extraordinary Expenses (Contigency)' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(139 AS INT) AS order_id, CAST(0 AS INT) AS section, CAST(CAST(NULL AS VARCHAR) AS VARCHAR) AS code, CAST('TOTAL OPERATING EXPENSES' AS VARCHAR) AS description, CAST('total' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(140 AS INT) AS order_id, CAST(0 AS INT) AS section, CAST(CAST(NULL AS VARCHAR) AS VARCHAR) AS code, CAST('NET INCOME (LOSS) BEFORE TAX' AS VARCHAR) AS description, CAST('total' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(141 AS INT) AS order_id, CAST(5 AS INT) AS section, CAST('5.09' AS VARCHAR) AS code, CAST('PROVISION FOR INCOME TAX' AS VARCHAR) AS description, CAST('leaf' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(142 AS INT) AS order_id, CAST(0 AS INT) AS section, CAST(CAST(NULL AS VARCHAR) AS VARCHAR) AS code, CAST('NET INCOME (LOSS) AFTER TAX' AS VARCHAR) AS description, CAST('total' AS VARCHAR) AS row_type
),

-- ── Pre-computed leaves from the staging model (already sign-corrected) ───────
leaves AS (
    SELECT load_date, order_id, CAST(amount AS DECIMAL(18,2)) AS amount
    FROM {{ ref('stg__po_sie_leaves') }}
    {% if var("target_date", "") != "" %}
    WHERE load_date = DATE '{{ var("target_date") }}'
    {% endif %}
),

-- ── Subtotals and totals (computed from leaves) ────────────────────────────────
subtotals AS (
    -- order  13: 4.01.4.1 = SUM(D14:D28)
    SELECT CAST( 13 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 14 AND 28 GROUP BY load_date
    -- order  29: 4.01.4.3 = SUM(D30:D44)
    UNION ALL SELECT CAST( 29 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 30 AND 44 GROUP BY load_date
    -- order  12: 4.01.4   = D13 + D29
    UNION ALL SELECT CAST( 12 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 14 AND 44 GROUP BY load_date
    -- order   8: 4.01     = D9+D10+D11+D12+D45
    UNION ALL SELECT CAST(  8 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 9 AND 45 GROUP BY load_date
    -- order  48: 4.02.6   = SUM(D49:D57)
    UNION ALL SELECT CAST( 48 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 49 AND 57 GROUP BY load_date
    -- order  46: 4.02     = SUM(D47:D48)
    UNION ALL SELECT CAST( 46 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 47 AND 57 GROUP BY load_date
    -- order  58: 4.03     = D59+D60+D61
    UNION ALL SELECT CAST( 58 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 59 AND 61 GROUP BY load_date
    -- order  62: TOTAL OP INCOME = D8+D46+D58
    UNION ALL SELECT CAST( 62 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 9 AND 61 GROUP BY load_date
    -- order  65: 5.01.2   = D66+D67+D68
    UNION ALL SELECT CAST( 65 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 66 AND 68 GROUP BY load_date
    -- order  69: 5.01.3   = D70
    UNION ALL SELECT CAST( 69 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id = 70 GROUP BY load_date
    -- order  64: 5.01     = D65+D69+D71
    UNION ALL SELECT CAST( 64 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 66 AND 71 GROUP BY load_date
    -- order  72: 5.02     = D73+D74+D75
    UNION ALL SELECT CAST( 72 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 73 AND 75 GROUP BY load_date
    -- order  77: 5.03.1   = SUM(D78:D84)
    UNION ALL SELECT CAST( 77 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 78 AND 84 GROUP BY load_date
    -- order  86: 5.03.2.4 = SUM(D87:D94)
    UNION ALL SELECT CAST( 86 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 87 AND 94 GROUP BY load_date
    -- order  85: 5.03.2   = D86
    UNION ALL SELECT CAST( 85 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 87 AND 94 GROUP BY load_date
    -- order  97: 5.03.5   = SUM(D98:D99)
    UNION ALL SELECT CAST( 97 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 98 AND 99 GROUP BY load_date
    -- order 100: 5.03.6  = D101+D102
    UNION ALL SELECT CAST(100 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 101 AND 102 GROUP BY load_date
    -- order 103: 5.03.7  = SUM(D104:D110)
    UNION ALL SELECT CAST(103 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 104 AND 110 GROUP BY load_date
    -- order 111: 5.03.8  = D112
    UNION ALL SELECT CAST(111 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id = 112 GROUP BY load_date
    -- order 113: 5.03.9  = SUM(D114:D133)
    UNION ALL SELECT CAST(113 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 114 AND 133 GROUP BY load_date
    -- order  76: 5.03     = 5.03.1..5.03.9
    UNION ALL SELECT CAST( 76 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 78 AND 133 GROUP BY load_date
    -- order 135: 5.04    = SUM(D136:D138)
    UNION ALL SELECT CAST(135 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 136 AND 138 GROUP BY load_date
    -- order 139: TOTAL OP EXPENSES = D64+D72+D76+D135
    UNION ALL SELECT CAST(139 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id BETWEEN 66 AND 138 GROUP BY load_date
    -- order 140: NET INCOME BEFORE TAX = D62 - D139
    UNION ALL SELECT CAST(140 AS INT) AS order_id, load_date,
           CAST(
               SUM(CASE WHEN order_id BETWEEN 9  AND 61  THEN amount ELSE CAST(0 AS DECIMAL(18,2)) END)
             - SUM(CASE WHEN order_id BETWEEN 66 AND 138 THEN amount ELSE CAST(0 AS DECIMAL(18,2)) END)
             AS DECIMAL(18,2)) AS amount
    FROM leaves GROUP BY load_date
    -- order 142: NET INCOME AFTER TAX = D140 - D141
    UNION ALL SELECT CAST(142 AS INT) AS order_id, load_date,
           CAST(
               SUM(CASE WHEN order_id BETWEEN 9  AND 61  THEN amount ELSE CAST(0 AS DECIMAL(18,2)) END)
             - SUM(CASE WHEN order_id BETWEEN 66 AND 141 THEN amount ELSE CAST(0 AS DECIMAL(18,2)) END)
             AS DECIMAL(18,2)) AS amount
    FROM leaves GROUP BY load_date
),

-- ── Combine leaves + subtotals into a single (load_date, order_id, amount) CTE ─
all_amounts AS (
    SELECT load_date, order_id, CAST(amount AS DECIMAL(18,2)) AS amount FROM leaves
    UNION ALL
    SELECT load_date, order_id, CAST(amount AS DECIMAL(18,2)) AS amount FROM subtotals
),

-- ── One load_date per target run (cross-join so every skeleton row has a date) ─
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

