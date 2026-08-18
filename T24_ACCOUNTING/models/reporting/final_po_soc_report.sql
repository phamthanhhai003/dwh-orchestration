{{ config(
    materialized='table',
    unique_key=['load_date', 'section', 'order_id'],
    incremental_strategy='merge',
    database='hive',
    schema='gold'
) }}

-- Division: po_soc (Consolidated Balance Sheet — Plan vs Actual, ACTUAL column only)
-- Strategy: custom multi-source consolidation (no seeds)
-- Source: hive.gold.final_assets_report + hive.gold.final_liabilities_report
-- Template: 'PO SOC (Consolidated AL )' sheet in docs/report-template/consolidate.xlsx
-- Business doc: docs/business-documentation/po_soc.md
-- Grain: 1 row per (load_date, section, order_id)
--
-- Sign convention:
--   - final_assets_report      : stored NEGATIVE → multiply by -1 (done in staging)
--   - final_liabilities_report : stored POSITIVE → use as-is (done in staging)
--
-- Branch filter: branch_name <> 'ATAURO' (Excel consolidates 14 of 15 gold branches).
--
-- Excel bug replicated: r82 Stationery gets +0.01 (handled in staging).
--
-- r84/r85/r86/r87 are shown in the report but NOT included in r77 or r94
-- per the Excel formula E77 = E78+E79+E80+E88 (deliberately excludes E84, E87).

WITH

-- ── PO SOC row skeleton ────────────────────────────────────────────────────────
row_skeleton AS (
    -- ═══ SECTION 1: ASSETS ═══
    SELECT CAST(  8 AS INT) AS order_id, CAST(1 AS INT) AS section, CAST('1.0' AS VARCHAR) AS code, CAST('A S S E T S :' AS VARCHAR) AS description, CAST('header' AS VARCHAR) AS row_type
    UNION ALL SELECT CAST(  9 AS INT), CAST(1 AS INT), CAST('1.01' AS VARCHAR), CAST('LIQUID FUNDS' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 10 AS INT), CAST(1 AS INT), CAST('1.01.1.1' AS VARCHAR), CAST('Cash in Vault/ on hand' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 11 AS INT), CAST(1 AS INT), CAST('1.01.1.2' AS VARCHAR), CAST('Cash in ATM Vault' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 12 AS INT), CAST(1 AS INT), CAST('1.01.2' AS VARCHAR), CAST('Due from Banco Central' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 13 AS INT), CAST(1 AS INT), CAST('1.01.3' AS VARCHAR), CAST('Items in course of collection' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 14 AS INT), CAST(1 AS INT), CAST('1.01.4' AS VARCHAR), CAST('Due from Commercial Banks' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 15 AS INT), CAST(1 AS INT), CAST('1.01.4.1' AS VARCHAR), CAST('Domestic Currency' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 16 AS INT), CAST(1 AS INT), CAST('1.01.4.2' AS VARCHAR), CAST('Foreign Currency (IDR)' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 18 AS INT), CAST(1 AS INT), CAST('1.02' AS VARCHAR), CAST('INVESTMENT' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 19 AS INT), CAST(1 AS INT), CAST('1.02.1' AS VARCHAR), CAST('Securities/SB (foreign)' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 20 AS INT), CAST(1 AS INT), CAST('1.04' AS VARCHAR), CAST('LOAN,ADVANCES & DISCOUNTS' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 21 AS INT), CAST(1 AS INT), CAST('1.04.1' AS VARCHAR), CAST('CURRENT LOAN' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 22 AS INT), CAST(1 AS INT), CAST('1.04.1.1.3.20' AS VARCHAR), CAST('Seasonal Crop Loans' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 23 AS INT), CAST(1 AS INT), CAST('1.04.1.1.3.30' AS VARCHAR), CAST('Other businees Laons' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 24 AS INT), CAST(1 AS INT), CAST('1.04.1.1.3.50' AS VARCHAR), CAST('Project Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 25 AS INT), CAST(1 AS INT), CAST('1.04.1.1.3.60' AS VARCHAR), CAST('Investment Loans' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 26 AS INT), CAST(1 AS INT), CAST('1.04.1.1.3.80' AS VARCHAR), CAST('Transport Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 27 AS INT), CAST(1 AS INT), CAST('1.04.1.1.3.85' AS VARCHAR), CAST('Agricultor Loans' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 28 AS INT), CAST(1 AS INT), CAST('1.04.1.1.4.10' AS VARCHAR), CAST('Microfinance Group Loans (Direct)' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 29 AS INT), CAST(1 AS INT), CAST('1.04.1.1.4.20' AS VARCHAR), CAST('Payroll Loans' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 30 AS INT), CAST(1 AS INT), CAST('1.04.1.1.4.30' AS VARCHAR), CAST('Loans to Employee and Staff' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 31 AS INT), CAST(1 AS INT), CAST('1.04.1.1.4.35' AS VARCHAR), CAST('Internal Staff Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 32 AS INT), CAST(1 AS INT), CAST('1.04.1.1.4.40' AS VARCHAR), CAST('Asuwa''in Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 33 AS INT), CAST(1 AS INT), CAST('1.04.1.1.4.50' AS VARCHAR), CAST('Bukae Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 34 AS INT), CAST(1 AS INT), CAST('1.04.1.1.4.60' AS VARCHAR), CAST('Pension Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 35 AS INT), CAST(1 AS INT), CAST('1.04.1.1.3.10' AS VARCHAR), CAST('Kreditu Fiar' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 36 AS INT), CAST(1 AS INT), CAST(CAST(NULL AS VARCHAR) AS VARCHAR), CAST('RBL' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 37 AS INT), CAST(1 AS INT), CAST('1.04.2' AS VARCHAR), CAST('PAST DUE LOANS' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 38 AS INT), CAST(1 AS INT), CAST('1.04.2.1.3.10' AS VARCHAR), CAST('Past Due Mark.Vend.D.L.' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 39 AS INT), CAST(1 AS INT), CAST('1.04.2.1.3.20' AS VARCHAR), CAST('Past Due Seas. C.Loans' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 40 AS INT), CAST(1 AS INT), CAST('1.04.2.1.3.30' AS VARCHAR), CAST('Past Due Other Busi. L.' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 41 AS INT), CAST(1 AS INT), CAST('1.04.2.1.3.50' AS VARCHAR), CAST('Past Due Project Loans' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 42 AS INT), CAST(1 AS INT), CAST('10.4.2.1.3.60' AS VARCHAR), CAST('Past Due Investment Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 43 AS INT), CAST(1 AS INT), CAST('1.04.2.1.3.80' AS VARCHAR), CAST('Past Due Transport Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 44 AS INT), CAST(1 AS INT), CAST('10.4.2.1.3.61' AS VARCHAR), CAST('Past Due Agriculture' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 45 AS INT), CAST(1 AS INT), CAST('1.04.2.1.4.10' AS VARCHAR), CAST('Past Due Microf. G.L.' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 46 AS INT), CAST(1 AS INT), CAST('1.04.2.1.4.20' AS VARCHAR), CAST('Past Due Payroll Loans' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 47 AS INT), CAST(1 AS INT), CAST('1.04.2.1.4.30' AS VARCHAR), CAST('Past Due L.to E.& Staff' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 48 AS INT), CAST(1 AS INT), CAST('1.04.2.1.4.35' AS VARCHAR), CAST('Past Due L.Internal Staff' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 49 AS INT), CAST(1 AS INT), CAST('1.04.2.1.4.40' AS VARCHAR), CAST('Past Due Asuwa''in Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 50 AS INT), CAST(1 AS INT), CAST('1.04.2.1.4.45' AS VARCHAR), CAST('Past Due Bukae Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 51 AS INT), CAST(1 AS INT), CAST('1.04.2.1.4.50' AS VARCHAR), CAST('Past Due Pension Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 52 AS INT), CAST(1 AS INT), CAST('1.04.2.1.4.55' AS VARCHAR), CAST('Past Due FIAR Loan' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 53 AS INT), CAST(1 AS INT), CAST(CAST(NULL AS VARCHAR) AS VARCHAR), CAST('Past Due RBL Loan Origination' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 54 AS INT), CAST(1 AS INT), CAST('1.04.3' AS VARCHAR), CAST('PROVISION FOR LOAN LOSSES' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 55 AS INT), CAST(1 AS INT), CAST('1.04.3.1.3' AS VARCHAR), CAST('Businees Interprises' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 56 AS INT), CAST(1 AS INT), CAST('1.04.3.1.4' AS VARCHAR), CAST('Others' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 57 AS INT), CAST(1 AS INT), CAST('1.04.3.1.5' AS VARCHAR), CAST('ECL Distribuition' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 58 AS INT), CAST(1 AS INT), CAST('1.05' AS VARCHAR), CAST('ACCOUNTS RECEIVABLE' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 59 AS INT), CAST(1 AS INT), CAST('1.05.1' AS VARCHAR), CAST('Interest Deposit Acrued' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 60 AS INT), CAST(1 AS INT), CAST('1.05.1' AS VARCHAR), CAST('Interest Loans Acrued' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 61 AS INT), CAST(1 AS INT), CAST('1.05.3' AS VARCHAR), CAST('Other Receivable' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 62 AS INT), CAST(1 AS INT), CAST('1.06' AS VARCHAR), CAST('FIXED ASSETS' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 63 AS INT), CAST(1 AS INT), CAST('1.06.2.1' AS VARCHAR), CAST('Offices' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 64 AS INT), CAST(1 AS INT), CAST('1.06.2.2' AS VARCHAR), CAST('Office Under Construction' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 65 AS INT), CAST(1 AS INT), CAST('1.06.2.3' AS VARCHAR), CAST('Leasehold Improvements' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 66 AS INT), CAST(1 AS INT), CAST('1.06.3' AS VARCHAR), CAST('Furniture and Fixtures' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 67 AS INT), CAST(1 AS INT), CAST('1.06.4' AS VARCHAR), CAST('Machinery and Equipment' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 68 AS INT), CAST(1 AS INT), CAST('1.06.5' AS VARCHAR), CAST('Vehicles' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 69 AS INT), CAST(1 AS INT), CAST('1.06.6' AS VARCHAR), CAST('Non Physical Assets' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 70 AS INT), CAST(1 AS INT), CAST('1.06.9' AS VARCHAR), CAST('RESERVE FOR DEPRECIATION' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 71 AS INT), CAST(1 AS INT), CAST('1.06.9.3' AS VARCHAR), CAST('Accum.Depr.Furneture & Fixture' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 72 AS INT), CAST(1 AS INT), CAST('1.06.9.4' AS VARCHAR), CAST('Accum.Depr.Mechinery & Equipment' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 73 AS INT), CAST(1 AS INT), CAST('1.06.9.5' AS VARCHAR), CAST('Accum.Depr.Vehicles' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 74 AS INT), CAST(1 AS INT), CAST('1.06.9.6' AS VARCHAR), CAST('Accum.Depr. Non-Physical Assets' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 75 AS INT), CAST(1 AS INT), CAST('1.06.9.7' AS VARCHAR), CAST('Accum.Depr. Building' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 76 AS INT), CAST(1 AS INT), CAST('1.06.9.8' AS VARCHAR), CAST('Accum.Depr. Leasehold Improvements' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 77 AS INT), CAST(1 AS INT), CAST('1.09' AS VARCHAR), CAST('OTHER ASSETS' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 78 AS INT), CAST(1 AS INT), CAST('1.09.1' AS VARCHAR), CAST('Prepaid Expenses' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 79 AS INT), CAST(1 AS INT), CAST('1.09.2' AS VARCHAR), CAST('Inter Branch Transactions (NET)' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 80 AS INT), CAST(1 AS INT), CAST('1.09.3' AS VARCHAR), CAST('Office Accounts' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 81 AS INT), CAST(1 AS INT), CAST('1.09.3.1' AS VARCHAR), CAST('Petty Cash Fund' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 82 AS INT), CAST(1 AS INT), CAST('1.09.3.3' AS VARCHAR), CAST('Stationery and Office Supplies' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 83 AS INT), CAST(1 AS INT), CAST('1.09.3.5' AS VARCHAR), CAST('Shortages' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 84 AS INT), CAST(1 AS INT), CAST('1.09.4' AS VARCHAR), CAST('Asset Held In Res. of Debt' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 85 AS INT), CAST(1 AS INT), CAST('1.09.4.2' AS VARCHAR), CAST('Assets Held In Res. of Debt' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 86 AS INT), CAST(1 AS INT), CAST('1.09.4.3' AS VARCHAR), CAST('Allowance for Probable Losses' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 87 AS INT), CAST(1 AS INT), CAST('1.09.5' AS VARCHAR), CAST('Items in Suspense' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 88 AS INT), CAST(1 AS INT), CAST('1.09.6' AS VARCHAR), CAST('Miscellaneous Assets' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST( 89 AS INT), CAST(1 AS INT), CAST('1.09.6.9.2.1' AS VARCHAR), CAST('Right Of Use Assets IFRS' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 90 AS INT), CAST(1 AS INT), CAST('1.09.6.9.2.2' AS VARCHAR), CAST('Differed Tax' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 91 AS INT), CAST(1 AS INT), CAST('1.09.6.9.2' AS VARCHAR), CAST('Intangible Assets (Assets in Progress)' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 92 AS INT), CAST(1 AS INT), CAST('1.09.6.9.4' AS VARCHAR), CAST('Other Assets-Depsit Payment Emp' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 93 AS INT), CAST(1 AS INT), CAST('1.09.6.9.7' AS VARCHAR), CAST('Deffered Charges' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST( 94 AS INT), CAST(0 AS INT), CAST(CAST(NULL AS VARCHAR) AS VARCHAR), CAST('Total Assets :' AS VARCHAR), CAST('total' AS VARCHAR)

    -- ═══ SECTION 2: LIABILITIES ═══
    UNION ALL SELECT CAST(104 AS INT), CAST(2 AS INT), CAST('2.0' AS VARCHAR), CAST('L I A B I L I T I E S :' AS VARCHAR), CAST('header' AS VARCHAR)
    UNION ALL SELECT CAST(106 AS INT), CAST(2 AS INT), CAST('2.04' AS VARCHAR), CAST('DEPOSITS' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST(107 AS INT), CAST(2 AS INT), CAST('2.04.1' AS VARCHAR), CAST('Demand Deposit:' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST(108 AS INT), CAST(2 AS INT), CAST('2.4.1.1.1' AS VARCHAR), CAST('Financial Institutions' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(109 AS INT), CAST(2 AS INT), CAST('2.4.1.1.2' AS VARCHAR), CAST('Government' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(110 AS INT), CAST(2 AS INT), CAST('2.4.1.1.3' AS VARCHAR), CAST('Business Enterprises' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(111 AS INT), CAST(2 AS INT), CAST('2.4.1.1.4' AS VARCHAR), CAST('Others' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(113 AS INT), CAST(2 AS INT), CAST('2.04.1.2' AS VARCHAR), CAST('Time Deposits' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST(114 AS INT), CAST(2 AS INT), CAST('2.04.1.2' AS VARCHAR), CAST('Government' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(115 AS INT), CAST(2 AS INT), CAST('2.04.1.4' AS VARCHAR), CAST('Others' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(117 AS INT), CAST(2 AS INT), CAST('2.04.1.3' AS VARCHAR), CAST('Other (Passbook Saving):' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST(118 AS INT), CAST(2 AS INT), CAST('2.04.1.1' AS VARCHAR), CAST('Financial Institutions' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(119 AS INT), CAST(2 AS INT), CAST('2.04.1.2' AS VARCHAR), CAST('Government' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(120 AS INT), CAST(2 AS INT), CAST('2.04.1.3' AS VARCHAR), CAST('Business Enterprises' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(121 AS INT), CAST(2 AS INT), CAST('2.04.1.4' AS VARCHAR), CAST('Others' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(122 AS INT), CAST(2 AS INT), CAST('2.04.1.4.1' AS VARCHAR), CAST('Passbook Saving' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(123 AS INT), CAST(2 AS INT), CAST('2.04.1.4.2' AS VARCHAR), CAST('Pledge Saving' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(124 AS INT), CAST(2 AS INT), CAST('2.04.1.4.5' AS VARCHAR), CAST('Saving Enderly' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(125 AS INT), CAST(2 AS INT), CAST('2.04.1.4.6' AS VARCHAR), CAST('Bolsa da Mae' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(126 AS INT), CAST(2 AS INT), CAST('2.04.1.4.7' AS VARCHAR), CAST('Deposito Asuwain' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(127 AS INT), CAST(2 AS INT), CAST('2.04.1.4.6' AS VARCHAR), CAST('Deposito Hau Nia Futuru' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(128 AS INT), CAST(2 AS INT), CAST('2.04.1.4.7' AS VARCHAR), CAST('Deposito Poupansa Emigrante' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(129 AS INT), CAST(2 AS INT), CAST('2.04.1.4.8' AS VARCHAR), CAST('Depozitu Matenek' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(130 AS INT), CAST(2 AS INT), CAST('2.04.1.4.9' AS VARCHAR), CAST('Depozitu Pensionista' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(131 AS INT), CAST(2 AS INT), CAST('2.04.1.4.0' AS VARCHAR), CAST('BDM Jerasaun Foun' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(132 AS INT), CAST(2 AS INT), CAST('2.04.1.5.0' AS VARCHAR), CAST('Depozitu Fiar' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(133 AS INT), CAST(2 AS INT), CAST('2.06' AS VARCHAR), CAST('Other Sundry Current Liabilities' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST(134 AS INT), CAST(2 AS INT), CAST('2.06.2' AS VARCHAR), CAST('Staff Expense' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(135 AS INT), CAST(2 AS INT), CAST('2.06.3.1' AS VARCHAR), CAST('Income Tax' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(136 AS INT), CAST(2 AS INT), CAST('2.06.3.1' AS VARCHAR), CAST('Differed Tax' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(137 AS INT), CAST(2 AS INT), CAST('2.06.3.3' AS VARCHAR), CAST('Other Tax' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(138 AS INT), CAST(2 AS INT), CAST('2.06.4' AS VARCHAR), CAST('Tax Payable' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(139 AS INT), CAST(2 AS INT), CAST('2.06.1' AS VARCHAR), CAST('Interest Acrued' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(140 AS INT), CAST(2 AS INT), CAST('2.06.6' AS VARCHAR), CAST('Item in Suspense' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(141 AS INT), CAST(2 AS INT), CAST('2.06.3' AS VARCHAR), CAST('Salary Payables' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(142 AS INT), CAST(2 AS INT), CAST('2.06.7' AS VARCHAR), CAST('Loan Suspense Migration' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(143 AS INT), CAST(2 AS INT), CAST('2.06.5' AS VARCHAR), CAST('Lease Liability IFRS' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(144 AS INT), CAST(2 AS INT), CAST('2.06.8' AS VARCHAR), CAST('Descricted Deposit (Bank Garansi) (*)' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(145 AS INT), CAST(2 AS INT), CAST('2.06.9' AS VARCHAR), CAST('Unearned Interest (Loan Fee)' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(146 AS INT), CAST(2 AS INT), CAST(CAST(NULL AS VARCHAR) AS VARCHAR), CAST(CAST(NULL AS VARCHAR) AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(147 AS INT), CAST(2 AS INT), CAST('2.08' AS VARCHAR), CAST('Other Liabilities/Inter Branch (HO-Branch)' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(148 AS INT), CAST(2 AS INT), CAST('2-08-1' AS VARCHAR), CAST('Others/Overages' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(149 AS INT), CAST(2 AS INT), CAST('2-08-2' AS VARCHAR), CAST('Overages ATM' AS VARCHAR), CAST('leaf' AS VARCHAR)

    -- ═══ SECTION 3: CAPITAL ═══
    UNION ALL SELECT CAST(150 AS INT), CAST(3 AS INT), CAST('3.0' AS VARCHAR), CAST('CAPITAL ACCOUNTS :' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST(151 AS INT), CAST(3 AS INT), CAST('3.01' AS VARCHAR), CAST('Provisioning' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(152 AS INT), CAST(3 AS INT), CAST('3.02' AS VARCHAR), CAST('Capital Paid-Up & Assigned' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(153 AS INT), CAST(3 AS INT), CAST('3-04-3-2' AS VARCHAR), CAST('Capital Reserve/Reserva Legais' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(154 AS INT), CAST(3 AS INT), CAST('3.06' AS VARCHAR), CAST('Profits / Losses' AS VARCHAR), CAST('subtotal' AS VARCHAR)
    UNION ALL SELECT CAST(155 AS INT), CAST(3 AS INT), CAST('3.06.1' AS VARCHAR), CAST('Previeous Financial Year' AS VARCHAR), CAST('leaf' AS VARCHAR)
    UNION ALL SELECT CAST(156 AS INT), CAST(3 AS INT), CAST('3.06.2' AS VARCHAR), CAST('Current Financial Year' AS VARCHAR), CAST('leaf' AS VARCHAR)

    -- ═══ TOTALS (section 0) ═══
    UNION ALL SELECT CAST(158 AS INT), CAST(0 AS INT), CAST(CAST(NULL AS VARCHAR) AS VARCHAR), CAST('Total Liabilities & Capital Accounts :' AS VARCHAR), CAST('total' AS VARCHAR)
    UNION ALL SELECT CAST(159 AS INT), CAST(0 AS INT), CAST(CAST(NULL AS VARCHAR) AS VARCHAR), CAST('Balance Check (Note:)' AS VARCHAR), CAST('total' AS VARCHAR)
),

-- ── Pre-computed leaves from the staging model (already sign-corrected) ─────
leaves AS (
    SELECT load_date, order_id, CAST(amount AS DECIMAL(18,2)) AS amount
    FROM {{ ref('stg__po_soc_leaves') }}
    {% if var("target_date", "") != "" %}
    WHERE load_date = DATE '{{ var("target_date") }}'
    {% endif %}
),

-- ── Subtotals and totals (computed from leaves) ────────────────────────────
subtotals AS (
    -- ASSETS
    -- r14 (1.01.4) = E15+E16
    SELECT CAST(14 AS INT) AS order_id, load_date, CAST(SUM(amount) AS DECIMAL(18,2)) AS amount FROM leaves WHERE order_id IN (15, 16) GROUP BY load_date
    -- r9 (1.01) = E10+E11+E12+E13+E14 = leaves 10..16
    UNION ALL SELECT CAST(9 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 10 AND 16 GROUP BY load_date
    -- r18 (1.02) = E19
    UNION ALL SELECT CAST(18 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id = 19 GROUP BY load_date
    -- r21 (1.04.1) = SUM(E22:E36)
    UNION ALL SELECT CAST(21 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 22 AND 36 GROUP BY load_date
    -- r37 (1.04.2) = SUM(E38:E53)
    UNION ALL SELECT CAST(37 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 38 AND 53 GROUP BY load_date
    -- r54 (1.04.3) = SUM(E55:E57)
    UNION ALL SELECT CAST(54 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 55 AND 57 GROUP BY load_date
    -- r20 (1.04) = E21+E37+E54 = leaves 22..57
    UNION ALL SELECT CAST(20 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 22 AND 57 GROUP BY load_date
    -- r58 (1.05) = E59+E60+E61
    UNION ALL SELECT CAST(58 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 59 AND 61 GROUP BY load_date
    -- r70 (1.06.9) = E71+E72+E73+E74+E75+E76
    UNION ALL SELECT CAST(70 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 71 AND 76 GROUP BY load_date
    -- r62 (1.06) = E63+E64+E65+E66+E67+E68+E69+E70 = leaves 63..76
    UNION ALL SELECT CAST(62 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 63 AND 76 GROUP BY load_date
    -- r80 (1.09.3) = E81+E82+E83
    UNION ALL SELECT CAST(80 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 81 AND 83 GROUP BY load_date
    -- r84 (1.09.4) = E85+E86  (NOT included in r77 or r94)
    UNION ALL SELECT CAST(84 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id IN (85, 86) GROUP BY load_date
    -- r88 (1.09.6) = E89+E90+E91+E92+E93
    UNION ALL SELECT CAST(88 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 89 AND 93 GROUP BY load_date
    -- r77 (1.09) = E78+E79+E80+E88 (EXCLUDES E84/E85/E86/E87)
    UNION ALL SELECT CAST(77 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id IN (78, 79, 81, 82, 83, 89, 90, 91, 92, 93) GROUP BY load_date
    -- r94 (TOTAL ASSETS) = E9+E18+E20+E58+E62+E77 = all asset leaves EXCEPT 84-87
    UNION ALL SELECT CAST(94 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE (order_id BETWEEN 10 AND 83 OR order_id BETWEEN 89 AND 93) GROUP BY load_date

    -- LIABILITIES
    -- r107 (2.04.1) = SUM(E108:E111)
    UNION ALL SELECT CAST(107 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 108 AND 111 GROUP BY load_date
    -- r113 (2.04.1.2) = E114+E115
    UNION ALL SELECT CAST(113 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id IN (114, 115) GROUP BY load_date
    -- r117 (2.04.1.3) = SUM(E118:E132)
    UNION ALL SELECT CAST(117 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 118 AND 132 GROUP BY load_date
    -- r106 (2.04) = E107+E117+E113 = leaves 108..132
    UNION ALL SELECT CAST(106 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 108 AND 132 GROUP BY load_date
    -- r133 (2.06) = SUM(E134:E145)
    UNION ALL SELECT CAST(133 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 134 AND 145 GROUP BY load_date

    -- CAPITAL
    -- r154 (3.06) = E155+E156
    UNION ALL SELECT CAST(154 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id IN (155, 156) GROUP BY load_date
    -- r150 (3.0) = SUM(E151:E154) = leaves 151..156
    UNION ALL SELECT CAST(150 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 151 AND 156 GROUP BY load_date

    -- TOTALS
    -- r158 (TOTAL LIAB+CAPITAL) = E106+E133+E147+E148+E150+E149
    -- = all liability/capital leaves 108..156 EXCLUDING r146 (phantom)
    UNION ALL SELECT CAST(158 AS INT), load_date, CAST(SUM(amount) AS DECIMAL(18,2)) FROM leaves WHERE order_id BETWEEN 108 AND 156 AND order_id <> 146 GROUP BY load_date
    -- r159 (BALANCE CHECK) = E158 - E94
    UNION ALL SELECT CAST(159 AS INT), load_date,
           CAST(
               SUM(CASE WHEN order_id BETWEEN 108 AND 156 AND order_id <> 146 THEN amount ELSE CAST(0 AS DECIMAL(18,2)) END)
             - SUM(CASE WHEN order_id BETWEEN 10 AND 83 OR order_id BETWEEN 89 AND 93 THEN amount ELSE CAST(0 AS DECIMAL(18,2)) END)
           AS DECIMAL(18,2)) AS amount
    FROM leaves GROUP BY load_date
),

-- ── Combine leaves + subtotals into a single (load_date, order_id, amount) CTE ─
all_amounts AS (
    SELECT load_date, order_id, CAST(amount AS DECIMAL(18,2)) AS amount FROM leaves
    UNION ALL
    SELECT load_date, order_id, CAST(amount AS DECIMAL(18,2)) AS amount FROM subtotals
),

-- ── One load_date per target run ──────────────────────────────────────────────
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
