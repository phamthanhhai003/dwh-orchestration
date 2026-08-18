{{ config(
    materialized='table',
    unique_key=['load_date', 'order_id'],
    incremental_strategy='merge',
    database='hive',
    schema='silver',
    tags=['po_soc', 'staging']
) }}

-- Staging: PO SOC leaf values
-- Computes leaf lookups by joining gold sources against a static
-- leaf_map (code, description → order_id).
--
-- Sign convention: assets NEGATIVE in gold → flip * -1; liabilities as-is.
-- Branch filter: branch_name <> 'ATAURO' (consolidation covers 14 branches).
-- Excel bug: r82 (Stationery&OfficeSupp.) gets +0.01 rounding adjustment.
--
-- Multi-source rows (2 gold rows → 1 order_id): r30, r47, r55, r60.
-- NULL-code rows (match by description): r36, r53.

WITH
asset_src AS (
    SELECT
        CAST('AST' AS VARCHAR)                          AS src_tag,
        load_date,
        CAST(code        AS VARCHAR)                    AS code,
        CAST(description AS VARCHAR)                    AS description,
        CAST(amount      AS DECIMAL(18,2)) * -1         AS amount
    FROM {{ ref('final_assets_report') }}
    WHERE branch_name <> 'ATAURO'
    {% if var("target_date", "") != "" %}
      AND load_date = DATE '{{ var("target_date") }}'
    {% endif %}
),

liability_src AS (
    SELECT
        CAST('LIB' AS VARCHAR)                          AS src_tag,
        load_date,
        CAST(code        AS VARCHAR)                    AS code,
        CAST(description AS VARCHAR)                    AS description,
        CAST(amount      AS DECIMAL(18,2))              AS amount
    FROM {{ ref('final_liabilities_report') }}
    WHERE branch_name <> 'ATAURO'
    {% if var("target_date", "") != "" %}
      AND load_date = DATE '{{ var("target_date") }}'
    {% endif %}
),

all_src AS (
    SELECT src_tag, load_date, code, description, amount FROM asset_src
    UNION ALL
    SELECT src_tag, load_date, code, description, amount FROM liability_src
),

-- Leaf lookup map: (src_tag, code, description) → order_id
-- Gold descriptions used — NOT PO SOC display labels.
leaf_map AS (
    -- ══ SECTION 1: ASSETS (from final_assets_report) ══
    -- 1.01 LIQUID FUNDS
    SELECT CAST('AST' AS VARCHAR) AS src_tag, CAST('1.01.1.1.1' AS VARCHAR) AS code, CAST('Cash in Vault' AS VARCHAR) AS description, CAST(10 AS INT) AS order_id
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.01.1.1.2' AS VARCHAR), CAST('Cash in ATM Vault' AS VARCHAR), CAST(11 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.01.2' AS VARCHAR), CAST('Due from Central Bank' AS VARCHAR), CAST(12 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.01.3' AS VARCHAR), CAST('Items in course of collect.' AS VARCHAR), CAST(13 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.01.4.1' AS VARCHAR), CAST('Domestic Currency' AS VARCHAR), CAST(15 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.01.4.3' AS VARCHAR), CAST('Foreign Currency' AS VARCHAR), CAST(16 AS INT)
    -- 1.02 INVESTMENT
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.02.1' AS VARCHAR), CAST('Securities' AS VARCHAR), CAST(19 AS INT)
    -- 1.04.1 CURRENT LOANS
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.1.1.3.20' AS VARCHAR), CAST('SeasonalCropLoans' AS VARCHAR), CAST(22 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.1.1.3.30' AS VARCHAR), CAST('OtherBusineesLoans' AS VARCHAR), CAST(23 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.1.1.3.50' AS VARCHAR), CAST('ProjectLoan' AS VARCHAR), CAST(24 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.1.1.3.60' AS VARCHAR), CAST('InvestmentLoans' AS VARCHAR), CAST(25 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.1.1.3.80' AS VARCHAR), CAST('TransportLoan' AS VARCHAR), CAST(26 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.1.1.3.85' AS VARCHAR), CAST('AgricultorLoan' AS VARCHAR), CAST(27 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.1.1.3.70' AS VARCHAR), CAST('MicrofinanceGroupL.' AS VARCHAR), CAST(28 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.1.1.4.20' AS VARCHAR), CAST('PayrollLoans' AS VARCHAR), CAST(29 AS INT)
    -- r30 multi-source: LoanstoEmpl.&Staff + InternalStaffLoan → both map to order 30
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.1.1.4.30' AS VARCHAR), CAST('LoanstoEmpl.&Staff' AS VARCHAR), CAST(30 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.1.1.4.35' AS VARCHAR), CAST('InternalStaffLoan' AS VARCHAR), CAST(30 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.1.1.4.40' AS VARCHAR), CAST('AsuwainLoan' AS VARCHAR), CAST(32 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.1.1.4.50' AS VARCHAR), CAST('BukaeLoan' AS VARCHAR), CAST(33 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.1.1.4.60' AS VARCHAR), CAST('PensionLoan' AS VARCHAR), CAST(34 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.1.1.4.65' AS VARCHAR), CAST('FIARLoan' AS VARCHAR), CAST(35 AS INT)
    -- r36 NULL-code
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST(CAST(NULL AS VARCHAR) AS VARCHAR), CAST('RBLLoanOrigination' AS VARCHAR), CAST(36 AS INT)
    -- 1.04.2 PAST DUE LOANS
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.2.1.3.10' AS VARCHAR), CAST('PastDueMark.Vend.D.L.' AS VARCHAR), CAST(38 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.2.1.3.20' AS VARCHAR), CAST('PastDueSeas.C.Loans' AS VARCHAR), CAST(39 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.2.1.3.30' AS VARCHAR), CAST('PastDueOtherBusi.L.' AS VARCHAR), CAST(40 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.2.1.3.50' AS VARCHAR), CAST('PastDueProjectLoans' AS VARCHAR), CAST(41 AS INT)
    -- r42, r43 not in gold → hardcoded 0 (not in leaf_map)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.2.1.4.37' AS VARCHAR), CAST('PastDueAgricultureLO' AS VARCHAR), CAST(44 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.2.1.4.10' AS VARCHAR), CAST('PastDueMicrof.G.L.' AS VARCHAR), CAST(45 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.2.1.4.20' AS VARCHAR), CAST('PastDuePayrollLoans' AS VARCHAR), CAST(46 AS INT)
    -- r47 multi-source: PastDueL.toE.&Staff + PastDueL.InternalStaff → both map to order 47
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.2.1.4.30' AS VARCHAR), CAST('PastDueL.toE.&Staff' AS VARCHAR), CAST(47 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.2.1.4.35' AS VARCHAR), CAST('PastDueL.InternalStaff' AS VARCHAR), CAST(47 AS INT)
    -- r48 hardcoded 0 (not in leaf_map)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.2.1.4.40' AS VARCHAR), CAST('PastDueAsuwainLoan' AS VARCHAR), CAST(49 AS INT)
    -- r50 duplicate code 1.04.2.1.4.35 — disambiguated by description
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.2.1.4.35' AS VARCHAR), CAST('PastDueBukaeLoan' AS VARCHAR), CAST(50 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.2.1.4.36' AS VARCHAR), CAST('PastDuePensionLoan' AS VARCHAR), CAST(51 AS INT)
    -- r52 not in gold → hardcoded 0 (not in leaf_map)
    -- r53 NULL-code
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST(CAST(NULL AS VARCHAR) AS VARCHAR), CAST('PastDueRBLLoanOrigination' AS VARCHAR), CAST(53 AS INT)
    -- 1.04.3 PROVISION FOR LOAN LOSSES
    -- r55 multi-source: BusineesInterprises + ECLDistribuition → both map to order 55
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.3.1.3' AS VARCHAR), CAST('BusineesInterprises' AS VARCHAR), CAST(55 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.3.1.5' AS VARCHAR), CAST('ECLDistribuition' AS VARCHAR), CAST(55 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.04.3.1.4' AS VARCHAR), CAST('Others' AS VARCHAR), CAST(56 AS INT)
    -- 1.05 ACCOUNTS RECEIVABLE
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.05.1.1' AS VARCHAR), CAST('DueFr.Banks&OtherFin. Inst' AS VARCHAR), CAST(59 AS INT)
    -- r60 multi-source: PerformingLoans + Non-PerformingLoans → both map to order 60
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.05.1.3.1' AS VARCHAR), CAST('PerformingLoans' AS VARCHAR), CAST(60 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.05.1.3.2' AS VARCHAR), CAST('Non-PerformingLoans' AS VARCHAR), CAST(60 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.05.3' AS VARCHAR), CAST('OtherReceivable' AS VARCHAR), CAST(61 AS INT)
    -- 1.06 FIXED ASSETS
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.06.2.1' AS VARCHAR), CAST('Offices/Buildings' AS VARCHAR), CAST(63 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.06.2.2' AS VARCHAR), CAST('OfficeUnderCunstruction' AS VARCHAR), CAST(64 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.06.2.3' AS VARCHAR), CAST('LeaseholdImprov.' AS VARCHAR), CAST(65 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.06.3' AS VARCHAR), CAST('FurnetureandFixtures' AS VARCHAR), CAST(66 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.06.4' AS VARCHAR), CAST('MachineryandEquip.' AS VARCHAR), CAST(67 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.06.5' AS VARCHAR), CAST('Vehicles' AS VARCHAR), CAST(68 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.06.6' AS VARCHAR), CAST('NonPhysicalAsset' AS VARCHAR), CAST(69 AS INT)
    -- 1.06.9 RESERVE FOR DEPRECIATION
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.06.9.3' AS VARCHAR), CAST('Accum.Depr.Fur.&Fixt.' AS VARCHAR), CAST(71 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.06.9.4' AS VARCHAR), CAST('Accum.Depr.Mech.&Eq.' AS VARCHAR), CAST(72 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.06.9.5' AS VARCHAR), CAST('Accum.Depr.Vehicles' AS VARCHAR), CAST(73 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.06.9.6' AS VARCHAR), CAST('Accum.Depr.Non-PhysicalAssets' AS VARCHAR), CAST(74 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.06.9.7' AS VARCHAR), CAST('Accum.Depr.Building' AS VARCHAR), CAST(75 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.06.9.8' AS VARCHAR), CAST('Accum.Depr.Leasehold' AS VARCHAR), CAST(76 AS INT)
    -- 1.09 OTHER ASSETS
    -- r78: code 1.09.1 is duplicate in gold — disambiguated by description
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.09.1' AS VARCHAR), CAST('PrepaidExpenses' AS VARCHAR), CAST(78 AS INT)
    -- r79 hardcoded 0 (not in leaf_map)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.09.3.1' AS VARCHAR), CAST('PettyCashFund' AS VARCHAR), CAST(81 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.09.3.3' AS VARCHAR), CAST('Stationery&OfficeSupp.' AS VARCHAR), CAST(82 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.09.3.5' AS VARCHAR), CAST('Shortages' AS VARCHAR), CAST(83 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.09.4.2' AS VARCHAR), CAST('AssetsHeldInRes.ofDebt' AS VARCHAR), CAST(85 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.09.4.3' AS VARCHAR), CAST('AllowanceforProbableLosses' AS VARCHAR), CAST(86 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.09.5' AS VARCHAR), CAST('ItemsinSuspense' AS VARCHAR), CAST(87 AS INT)
    -- 1.09.6 Miscellaneous Assets
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.09.6.9.2.1' AS VARCHAR), CAST('Right Of Use Assets IFRS' AS VARCHAR), CAST(89 AS INT)
    -- r90 hardcoded 0 (not in leaf_map)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.09.6.9.2' AS VARCHAR), CAST('IntangibleAssets(AssetsinProgress)' AS VARCHAR), CAST(91 AS INT)
    UNION ALL SELECT CAST('AST' AS VARCHAR), CAST('1.09.6.9.4' AS VARCHAR), CAST('OtherAssets-DepsitPaymentEmp' AS VARCHAR), CAST(92 AS INT)
    -- r93 hardcoded 0 (not in leaf_map)

    -- ══ SECTION 2: LIABILITIES (from final_liabilities_report) ══
    -- 2.04.1 Demand Deposit
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.4.1.1.1' AS VARCHAR), CAST('Financial Institutions' AS VARCHAR), CAST(108 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.4.1.1.2' AS VARCHAR), CAST('Government' AS VARCHAR), CAST(109 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.4.1.1.3' AS VARCHAR), CAST('Business Enterprises' AS VARCHAR), CAST(110 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.4.1.1.4' AS VARCHAR), CAST('Others' AS VARCHAR), CAST(111 AS INT)
    -- 2.04.1.2 Time Deposits
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.04.1.2.2' AS VARCHAR), CAST('Government' AS VARCHAR), CAST(114 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.4.1.2.4' AS VARCHAR), CAST('Others' AS VARCHAR), CAST(115 AS INT)
    -- 2.04.1.3 Passbook Saving
    -- r118, r119 not in gold → hardcoded 0 (not in leaf_map)
    -- r120: code 2.04.1.3 is duplicate — disambiguated by description
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.04.1.3' AS VARCHAR), CAST('Business Enterprises' AS VARCHAR), CAST(120 AS INT)
    -- r121 missing formula in PO SOC → hardcoded 0 (not in leaf_map)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.04.1.4.1' AS VARCHAR), CAST('Passbook Saving' AS VARCHAR), CAST(122 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.04.1.4.2' AS VARCHAR), CAST('Pledge Saving' AS VARCHAR), CAST(123 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.04.1.4.5' AS VARCHAR), CAST('Saving Enderly' AS VARCHAR), CAST(124 AS INT)
    -- r125/r127: duplicate code 2.04.1.4.6 — disambiguated by description
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.04.1.4.6' AS VARCHAR), CAST('Bolsa da Mae' AS VARCHAR), CAST(125 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.04.1.4.7' AS VARCHAR), CAST('Deposito Asuwain' AS VARCHAR), CAST(126 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.04.1.4.6' AS VARCHAR), CAST('Deposito Hau Nia Futuru' AS VARCHAR), CAST(127 AS INT)
    -- r128: gold code has trailing dot
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.04.1.4.' AS VARCHAR), CAST('Deposito Poupansa Emigrante' AS VARCHAR), CAST(128 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.04.1.4.8' AS VARCHAR), CAST('Depozitu Matenek' AS VARCHAR), CAST(129 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.04.1.4.9' AS VARCHAR), CAST('Depozitu Pensionista' AS VARCHAR), CAST(130 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.04.1.4.0' AS VARCHAR), CAST('BDM Jerasaun Foun' AS VARCHAR), CAST(131 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.04.1.5.0' AS VARCHAR), CAST('Depozitu Fiar' AS VARCHAR), CAST(132 AS INT)
    -- 2.06 Other Sundry Current Liabilities
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.06.2' AS VARCHAR), CAST('ReserveFor Bonus & Atribu BOD' AS VARCHAR), CAST(134 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.06.3.1' AS VARCHAR), CAST('Income Tax' AS VARCHAR), CAST(135 AS INT)
    -- r136 not in gold → hardcoded 0 (not in leaf_map)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.06.3.2' AS VARCHAR), CAST('Other Taxes' AS VARCHAR), CAST(137 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.06.4.2' AS VARCHAR), CAST('Collected Tax' AS VARCHAR), CAST(138 AS INT)
    -- r139/r143: duplicate code 2.06.5 — disambiguated by description
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.06.5' AS VARCHAR), CAST('Interest Accrued' AS VARCHAR), CAST(139 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.06.6' AS VARCHAR), CAST('Item In Suspens' AS VARCHAR), CAST(140 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.06.3' AS VARCHAR), CAST('Salary Payable' AS VARCHAR), CAST(141 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.06.4' AS VARCHAR), CAST('Loan Suspense Migration' AS VARCHAR), CAST(142 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.06.5' AS VARCHAR), CAST('Lease Liability IFRS' AS VARCHAR), CAST(143 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.06.8' AS VARCHAR), CAST('RESTRICTED DEPOSIT' AS VARCHAR), CAST(144 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2.06.9' AS VARCHAR), CAST('Unearned Interest' AS VARCHAR), CAST(145 AS INT)
    -- 2.08 Other Liabilities / Inter Branch
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2-08-1-1' AS VARCHAR), CAST('Inter Branch Transac (Net)' AS VARCHAR), CAST(147 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2-08-1' AS VARCHAR), CAST('Others/Overages' AS VARCHAR), CAST(148 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('2-08-2' AS VARCHAR), CAST('Overages ATM' AS VARCHAR), CAST(149 AS INT)

    -- ══ SECTION 3: CAPITAL (from final_liabilities_report) ══
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('3.01' AS VARCHAR), CAST('Provisioning' AS VARCHAR), CAST(151 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('3.02' AS VARCHAR), CAST('Capital Paid-Up & Assigned' AS VARCHAR), CAST(152 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('3-04-3-2' AS VARCHAR), CAST('Capital Reserve/Reserve Legais' AS VARCHAR), CAST(153 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('3.06.1' AS VARCHAR), CAST('Previeous Financial Year' AS VARCHAR), CAST(155 AS INT)
    UNION ALL SELECT CAST('LIB' AS VARCHAR), CAST('3.06.2' AS VARCHAR), CAST('Current Financial Year' AS VARCHAR), CAST(156 AS INT)
),

-- NULL-safe JOIN for all leaves (handles NULL-code rows 36, 53)
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
)

-- r82 Excel bug: SOC!D78 = Asset!AE114 + 0.01 — replicate exactly
SELECT load_date, order_id,
    CASE WHEN order_id = 82 THEN CAST(amount + CAST(0.01 AS DECIMAL(18,2)) AS DECIMAL(18,2))
         ELSE CAST(amount AS DECIMAL(18,2))
    END AS amount,
    CURRENT_TIMESTAMP AS updated_at
FROM leaves_joined

