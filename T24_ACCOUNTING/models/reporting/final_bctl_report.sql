{{ config(
    materialized='table',
    unique_key=['load_date', 'bctl_section', 'code', 'description'],
    incremental_strategy='merge',
    database='hive',
    schema='gold'
) }}

-- Division  : bctl (BCTL Consolidated Chart of Accounts)
-- Grain     : 1 row per (load_date, bctl_section, order_id)
-- Sections  :
--   1 → final_assets_report      (ASSETS,      T24 sign ×-1)
--   2 → final_liabilities_report (LIABILITIES, codes 2.x / 2-x only)
--   3 → final_liabilities_report (CAPITAL,     codes 3.x / 3-x, profit overridden)
--   4 → final_income_report      (INCOME,      positive)
--   5 → final_expense_report     (EXPENSES,    T24 sign ×-1)
--   7 → skeleton only            (CONTINGENT,  3 leaves hardcoded → NULL; rest 0 → NULL)
--   8 → skeleton only            (MEMORANDA,   8.4.x hardcoded → NULL; 8.11.2 from pipeline)
--
-- Grand totals overridden (source leaves 0-placeholder):
--   1.0  = SUM first-level 1.x subtotals
--   2.0  = SUM first-level 2.x + capital total
--   3.0  = SUM 3.x non-profit + 3.06 (overridden)
--   3.06 = 3.06.1 + 3.06.2 (current-year profit)
--   4.0  = SUM all income rows (excl header)
--   5.0  = SUM all expense rows (excl header)
--   7 subtotals  computed from children (NULL when children are NULL)
--   8 subtotals  computed from children (NULL when children are NULL)
--
-- Section 7 NULL leaves (manual entry, no pipeline source):
--   7.1.1.2  Other (Guarantees)         → hardcoded in Excel
--   7.3      Unused Lines of Credit     → hardcoded in Excel
--   7.4.1    Domestic Currency          → hardcoded in Excel
--
-- Section 8 NULL leaves (manual entry, no pipeline source):
--   8.4.1    Past Due 90 Days           → hardcoded in Excel
--   8.4.2    Past Due 90-180 Days       → hardcoded in Excel
--   8.4.3    Past Due 180-270 Days      → hardcoded in Excel
--   8.4.4    Write-Offs                 → complex cumulative formula in Excel
--
-- Section 8 computable from pipeline:
--   8.11.2   Other Loans = SOC!D29+SOC!D45
--          = Asset!AE60+AE61 + Asset!AE77+AE78
--          = final_assets_report order_id IN (56,57) + order_id IN (73,74)
--            (Loans to E&Staff + Internal Staff + Past Due versions)

WITH

-- ── SECTION 1: ASSETS ────────────────────────────────────────────────────────
assets_raw AS (
    SELECT
        load_date,
        CAST(1 AS INT)                           AS bctl_section,
        order_id,
        CAST(code        AS VARCHAR)             AS code,
        CAST(description AS VARCHAR)             AS description,
        SUM(CAST(amount AS DECIMAL(18,2))) * -1  AS amount
    FROM {{ ref('final_assets_report') }}
    {% if var("target_date", "") != "" %}
    WHERE load_date = DATE '{{ var("target_date") }}'
    {% endif %}
    GROUP BY load_date, order_id, CAST(code AS VARCHAR), CAST(description AS VARCHAR)
),

-- ── SECTIONS 2+3: LIABILITIES + CAPITAL (raw) ────────────────────────────────
liabilities_raw AS (
    SELECT
        load_date,
        order_id,
        CAST(code        AS VARCHAR)             AS code,
        CAST(description AS VARCHAR)             AS description,
        SUM(CAST(amount AS DECIMAL(18,2)))       AS amount
    FROM {{ ref('final_liabilities_report') }}
    {% if var("target_date", "") != "" %}
    WHERE load_date = DATE '{{ var("target_date") }}'
    {% endif %}
    GROUP BY load_date, order_id, CAST(code AS VARCHAR), CAST(description AS VARCHAR)
),

-- ── CURRENT YEAR PROFIT → override 3.06.2 ────────────────────────────────────
total_income_cy AS (
    SELECT load_date, SUM(CAST(amount AS DECIMAL(18,2))) AS val
    FROM {{ ref('final_income_report') }}
    WHERE CAST(code AS VARCHAR) != '4.0'
    {% if var("target_date", "") != "" %} AND load_date = DATE '{{ var("target_date") }}' {% endif %}
    GROUP BY 1
),
total_expense_cy AS (
    SELECT load_date, SUM(CAST(amount AS DECIMAL(18,2))) * -1 AS val
    FROM {{ ref('final_expense_report') }}
    WHERE CAST(code AS VARCHAR) != '5.0'
    {% if var("target_date", "") != "" %} AND load_date = DATE '{{ var("target_date") }}' {% endif %}
    GROUP BY 1
),
current_year_profit AS (
    SELECT i.load_date,
           CAST(COALESCE(i.val, 0) - COALESCE(e.val, 0) AS DECIMAL(18,2)) AS profit
    FROM total_income_cy i
    LEFT JOIN total_expense_cy e ON i.load_date = e.load_date
),

liabilities_with_profit AS (
    SELECT
        lr.load_date,
        lr.order_id,
        lr.code,
        lr.description,
        CASE
            WHEN lr.code = '3.06.2'
                THEN COALESCE(cyp.profit, CAST(0 AS DECIMAL(18,2)))
            ELSE lr.amount
        END AS amount
    FROM liabilities_raw lr
    LEFT JOIN current_year_profit cyp ON lr.load_date = cyp.load_date
),

-- ── SECTION 2: LIABILITIES (codes 2.x / 2-x) ─────────────────────────────────
section2 AS (
    SELECT load_date, CAST(2 AS INT) AS bctl_section, order_id, code, description, amount
    FROM liabilities_with_profit
    WHERE code LIKE '2.%' OR code LIKE '2-%' OR code = '2.0'
),

-- ── SECTION 3: CAPITAL (codes 3.x / 3-x) ─────────────────────────────────────
section3 AS (
    SELECT load_date, CAST(3 AS INT) AS bctl_section, order_id, code, description, amount
    FROM liabilities_with_profit
    WHERE code LIKE '3.%' OR code LIKE '3-%' OR code = '3.0'
),

-- ── SECTION 4: INCOME ────────────────────────────────────────────────────────
section4 AS (
    SELECT
        load_date, CAST(4 AS INT) AS bctl_section, order_id,
        CAST(code AS VARCHAR) AS code, CAST(description AS VARCHAR) AS description,
        SUM(CAST(amount AS DECIMAL(18,2))) AS amount
    FROM {{ ref('final_income_report') }}
    {% if var("target_date", "") != "" %} WHERE load_date = DATE '{{ var("target_date") }}' {% endif %}
    GROUP BY load_date, order_id, CAST(code AS VARCHAR), CAST(description AS VARCHAR)
),

-- ── SECTION 5: EXPENSE ───────────────────────────────────────────────────────
section5 AS (
    SELECT
        load_date, CAST(5 AS INT) AS bctl_section, order_id,
        CAST(code AS VARCHAR) AS code, CAST(description AS VARCHAR) AS description,
        SUM(CAST(amount AS DECIMAL(18,2))) * -1 AS amount
    FROM {{ ref('final_expense_report') }}
    {% if var("target_date", "") != "" %} WHERE load_date = DATE '{{ var("target_date") }}' {% endif %}
    GROUP BY load_date, order_id, CAST(code AS VARCHAR), CAST(description AS VARCHAR)
),

-- ── SECTION 7: CONTINGENT — skeleton with NULL leaves ────────────────────────
-- 7.1.1.2, 7.3, 7.4.1 : hardcoded in Excel, no pipeline → NULL
-- All other leaves     : value = 0 in Excel   → NULL (consistent treatment)
-- Subtotals (7, 7.1, 7.1.1, 7.4, 7.5, 7.5.1) : computed from children → NULL while children NULL
dates AS (SELECT DISTINCT load_date FROM assets_raw),

section7 AS (
    SELECT load_date, CAST(7 AS INT) AS bctl_section,
           v.order_id, v.code, v.description,
           CAST(NULL AS DECIMAL(18,2)) AS amount
    FROM dates
    CROSS JOIN (VALUES
        (1,  '7',       'CONTINGENT ACCOUNTS'),
        (2,  '7.1',     'Acceptances Guarantees & Letters of Credit'),
        (3,  '7.1.1',   'Guarantees Issued'),
        (4,  '7.1.1.1', 'Commercial Paper'),
        (5,  '7.1.1.2', 'Other'),           -- hardcoded 816,820.52 in Excel
        (6,  '7.1.2',   'Letters of Credit Issued'),
        (7,  '7.2',     'Other Confirmed Paper'),
        (8,  '7.3',     'Unused Lines of Credit'),  -- hardcoded 64,473,597.79
        (9,  '7.4',     'Undisbursed Loan Funds'),
        (10, '7.4.1',   'Domestic Currency'),        -- hardcoded 26,639,361.50
        (11, '7.4.2',   'Foreign Currency'),
        (12, '7.5',     'Commitments'),
        (13, '7.5.1',   'To Grant Loans'),
        (14, '7.5.1.1', 'Domestic Currency'),
        (15, '7.5.1.2', 'Foreign Currency'),
        (16, '7.5.2',   'To Buy Securities'),
        (17, '7.5.3',   'To Sell Securities'),
        (18, '7.5.4',   'To Lease'),
        (19, '7.6',     'Pending Litigation'),
        (20, '7.7',     'Assets Sold with Recourse'),
        (21, '7.8',     'Other')
    ) AS v(order_id, code, description)
),

-- ── SECTION 8: MEMORANDA — skeleton, 8.11.2 from pipeline ────────────────────
-- 8.4.1-8.4.4 : hardcoded / cumulative formula in Excel → NULL
-- 8.11.2      : SOC!D29+SOC!D45 = Asset!AE60+AE61+AE77+AE78
--             = final_assets_report order_id IN (56,57,73,74) × -1 (T24 sign)
-- All other leaves = 0 in Excel → NULL (consistent)
-- Subtotals computed from children → NULL while children NULL

loan_to_staff_811_2 AS (
    SELECT
        load_date,
        SUM(CAST(amount AS DECIMAL(18,2))) * -1 AS amount
    FROM {{ ref('final_assets_report') }}
    WHERE order_id IN (56, 57, 73, 74)
    -- 56 = Loans to Empl.&Staff    (Asset!AE60)
    -- 57 = Internal Staff Loan     (Asset!AE61)
    -- 73 = Past Due L.to E.&Staff  (Asset!AE77)
    -- 74 = Past Due L.InternalStaff(Asset!AE78)
    {% if var("target_date", "") != "" %} AND load_date = DATE '{{ var("target_date") }}' {% endif %}
    GROUP BY load_date
),

section8_skeleton AS (
    SELECT load_date, CAST(8 AS INT) AS bctl_section,
           v.order_id, v.code, v.description
    FROM dates
    CROSS JOIN (VALUES
        (1,  '8',       'Memoranda Items'),
        (2,  '8.1',     'Authorized Capital'),
        (3,  '8.2',     'Subscribed Capital'),
        (4,  '8.3',     'Paid-Up/Assigned Capital'),
        (5,  '8.3.1',   'Domestic Currency'),
        (6,  '8.3.2',   'Foreign Currency'),
        (7,  '8.4',     'Past Due Loan'),
        (8,  '8.4.1',   '90 Days'),
        (9,  '8.4.2',   '90 - 180 Days'),
        (10, '8.4.3',   '180 - 270 Days'),
        (11, '8.4.4',   'Write-Offs'),
        (12, '8.5',     'Interest Capitalized During the Month'),
        (13, '8.6',     'Charged off Assets During the Month'),
        (14, '8.6.1',   'Loans'),
        (15, '8.6.2',   'Other'),
        (16, '8.7',     'Bad Loans Recovered During the Month'),
        (17, '8.8',     'Syndicated Loans'),
        (18, '8.8.1',   'Lead Institution'),
        (19, '8.8.2',   'Participating Institution'),
        (20, '8.9',     'Loan Participation'),
        (21, '8.9.1',   'Acquired by the Institution'),
        (22, '8.9.1.1', 'With Recourse'),
        (23, '8.9.1.2', 'Without Recourse'),
        (24, '8.9.2',   'Conveyed to Others'),
        (25, '8.9.2.1', 'With Recourse'),
        (26, '8.9.2.2', 'Without Recourse'),
        (27, '8.10',    'Credit Card Receivables'),
        (28, '8.10.1',  'Domestic Currency'),
        (29, '8.10.2',  'Foreign Currency'),
        (30, '8.11',    'Loan to Staff'),
        (31, '8.11.1',  'Mortgage Loans'),
        (32, '8.11.2',  'Other Loans'),    -- = SOC!D29+SOC!D45 (from pipeline)
        (33, '8.12',    'Total Credit Facilities'),
        (34, '8.13',    'Investment in Shares'),
        (35, '8.13.1',  'Quoted'),
        (36, '8.13.2',  'Unquoted'),
        (37, '8.14',    'Own Securities Assigned as Collateral'),
        (38, '8.14.1',  'Ordinarily Eligible as Liquid Securities'),
        (39, '8.14.2',  'Other'),
        (40, '8.15',    'Unused Portion of L/C in Favor Own Institution'),
        (41, '8.16',    'Assets Held in Trust'),
        (42, '8.17',    'Funding to Connected Parties'),
        (43, '8.17.1',  'Placements'),
        (44, '8.17.2',  'Investments'),
        (45, '8.17.3',  'Credits'),
        (46, '8.17.4',  'Accounts Receivables'),
        (47, '8.17.5',  'Other'),
        (48, '8.18',    'Funding from Connected Parties'),
        (49, '8.18.1',  'Capital'),
        (50, '8.18.2',  'Deposits'),
        (51, '8.18.3',  'Accounts Payable'),
        (52, '8.18.4',  'Other')
    ) AS v(order_id, code, description)
),

section8 AS (
    SELECT
        s.load_date,
        s.bctl_section,
        s.order_id,
        s.code,
        s.description,
        CASE
            -- 8.11.2: computable from pipeline
            WHEN s.code = '8.11.2'
                THEN COALESCE(l.amount, CAST(0 AS DECIMAL(18,2)))
            -- 8.11: parent = 8.11.1(0) + 8.11.2
            WHEN s.code = '8.11'
                THEN COALESCE(l.amount, CAST(0 AS DECIMAL(18,2)))
            -- All other rows: NULL (hardcoded or zero in Excel, no pipeline source)
            ELSE CAST(NULL AS DECIMAL(18,2))
        END AS amount
    FROM section8_skeleton s
    LEFT JOIN loan_to_staff_811_2 l ON s.load_date = l.load_date
),

-- ── UNION ALL SECTIONS ────────────────────────────────────────────────────────
all_sections AS (
    SELECT load_date, bctl_section, order_id, code, description, amount FROM assets_raw
    UNION ALL
    SELECT load_date, bctl_section, order_id, code, description, amount FROM section2
    UNION ALL
    SELECT load_date, bctl_section, order_id, code, description, amount FROM section3
    UNION ALL
    SELECT load_date, bctl_section, order_id, code, description, amount FROM section4
    UNION ALL
    SELECT load_date, bctl_section, order_id, code, description, amount FROM section5
    UNION ALL
    SELECT load_date, bctl_section, order_id, code, description, amount FROM section7
    UNION ALL
    SELECT load_date, bctl_section, order_id, code, description, amount FROM section8
),

-- ── GRAND TOTAL OVERRIDES ─────────────────────────────────────────────────────

-- 1.0 ASSETS = SUM first-level 1.x subtotals (Excel Row 7: I8+I47+I68+I76+I117+I127+I140)
assets_grand_total AS (
    SELECT load_date, SUM(amount) AS total
    FROM all_sections
    WHERE bctl_section = 1
      AND code LIKE '1.%' AND code NOT LIKE '1.%.%' AND code != '1.0'
    GROUP BY load_date
),

-- 4.0 INCOME = SUM all income rows excl header
income_grand_total AS (
    SELECT load_date, SUM(amount) AS total
    FROM all_sections
    WHERE bctl_section = 4 AND code != '4.0'
    GROUP BY load_date
),

-- 5.0 EXPENSE = SUM all expense rows excl header
expense_grand_total AS (
    SELECT load_date, SUM(amount) AS total
    FROM all_sections
    WHERE bctl_section = 5 AND code != '5.0'
    GROUP BY load_date
),

-- 3.06 = 3.06.1 + 3.06.2 (overridden)
capital_3_06_total AS (
    SELECT load_date, SUM(amount) AS total
    FROM all_sections
    WHERE bctl_section = 3 AND code IN ('3.06.1', '3.06.2')
    GROUP BY load_date
),

-- 3.0 CAPITAL = first-level 3.x + dash-coded 3-x + 3.06(overridden)
capital_3_0_total AS (
    SELECT
        a.load_date,
        COALESCE(SUM(CASE
            WHEN a.code LIKE '3.0%' AND a.code NOT LIKE '3.0%.%'
             AND a.code NOT IN ('3.0', '3.06') THEN a.amount
            WHEN a.code LIKE '3-%'             THEN a.amount
            ELSE CAST(0 AS DECIMAL(18,2))
        END), CAST(0 AS DECIMAL(18,2)))
        + COALESCE(c306.total, CAST(0 AS DECIMAL(18,2))) AS total
    FROM all_sections a
    LEFT JOIN capital_3_06_total c306 ON a.load_date = c306.load_date
    WHERE a.bctl_section = 3
    GROUP BY a.load_date, c306.total
),

-- 2.0 LIABILITIES (incl. Capital) = first-level 2.x + capital total
liabilities_grand_total AS (
    SELECT
        a.load_date,
        COALESCE(SUM(CASE
            WHEN a.bctl_section = 2
             AND a.code LIKE '2.%' AND a.code NOT LIKE '2.%.%'
             AND a.code != '2.0' THEN a.amount
            ELSE CAST(0 AS DECIMAL(18,2))
        END), CAST(0 AS DECIMAL(18,2)))
        + COALESCE(c30.total, CAST(0 AS DECIMAL(18,2))) AS total
    FROM all_sections a
    LEFT JOIN capital_3_0_total c30 ON a.load_date = c30.load_date
    WHERE a.bctl_section IN (2, 3)
    GROUP BY a.load_date, c30.total
)

-- ── FINAL OUTPUT ─────────────────────────────────────────────────────────────
SELECT
    a.load_date,
    a.bctl_section,
    a.order_id,
    a.code,
    a.description,
    CONCAT(
        CAST(CAST(SUBSTR(a.load_date, 9, 2) AS INTEGER) AS VARCHAR),
        CASE
            WHEN CAST(SUBSTR(a.load_date, 9, 2) AS INTEGER) IN (11, 12, 13) THEN 'th'
            WHEN MOD(CAST(SUBSTR(a.load_date, 9, 2) AS INTEGER), 10) = 1 THEN 'st'
            WHEN MOD(CAST(SUBSTR(a.load_date, 9, 2) AS INTEGER), 10) = 2 THEN 'nd'
            WHEN MOD(CAST(SUBSTR(a.load_date, 9, 2) AS INTEGER), 10) = 3 THEN 'rd'
            ELSE 'th'
        END,
        ' ',
        CASE CAST(SUBSTR(a.load_date, 6, 2) AS INTEGER)
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
        SUBSTR(a.load_date, 1, 4)
    ) AS load_date_label,
    CAST(
        CASE
            WHEN a.bctl_section = 1 AND a.code = '1.0'
                THEN COALESCE(agt.total, CAST(0 AS DECIMAL(18,2)))
            WHEN a.bctl_section = 2 AND a.code = '2.0'
                THEN COALESCE(lgt.total, CAST(0 AS DECIMAL(18,2)))
            WHEN a.bctl_section = 3 AND a.code = '3.0'
                THEN COALESCE(c30.total, CAST(0 AS DECIMAL(18,2)))
            WHEN a.bctl_section = 3 AND a.code = '3.06'
                THEN COALESCE(c306.total, CAST(0 AS DECIMAL(18,2)))
            WHEN a.bctl_section = 4 AND a.code = '4.0'
                THEN COALESCE(igt.total, CAST(0 AS DECIMAL(18,2)))
            WHEN a.bctl_section = 5 AND a.code = '5.0'
                THEN COALESCE(egt.total, CAST(0 AS DECIMAL(18,2)))
            ELSE a.amount  -- includes NULL for section 7/8 hardcoded rows
        END
    AS DECIMAL(18,2)) AS amount,
    CURRENT_TIMESTAMP AS updated_at
FROM all_sections a
LEFT JOIN assets_grand_total      agt  ON a.load_date = agt.load_date  AND a.bctl_section = 1 AND a.code = '1.0'
LEFT JOIN liabilities_grand_total lgt  ON a.load_date = lgt.load_date  AND a.bctl_section = 2 AND a.code = '2.0'
LEFT JOIN capital_3_0_total       c30  ON a.load_date = c30.load_date  AND a.bctl_section = 3 AND a.code = '3.0'
LEFT JOIN capital_3_06_total      c306 ON a.load_date = c306.load_date AND a.bctl_section = 3 AND a.code = '3.06'
LEFT JOIN income_grand_total      igt  ON a.load_date = igt.load_date  AND a.bctl_section = 4 AND a.code = '4.0'
LEFT JOIN expense_grand_total     egt  ON a.load_date = egt.load_date  AND a.bctl_section = 5 AND a.code = '5.0'

ORDER BY a.load_date, a.bctl_section, a.order_id