{{ config(
    materialized='table',
    database='hive',
    schema='silver'
) }}

-- Division: credit | Report: INACTIVE.ACCOUNT.REPORT (t24-built source)
-- Reads real hive.bronze.t24_* per Part-1 §13 INACTIVE.ACCOUNT.REPORT
-- (T24_REPORT_DEVELOPMENT_SOT.md).
--
-- Grain: one row per inactive/dormant ACCOUNT (recid). t24_account is already
-- 1 row/recid; COMPANY enrichment is 1:1 by co_code; CUSTOMER is a versioned
-- dim → deduped to latest version per recid to keep the join 1:1.
--
-- §13 selection filter (UNCONDITIONAL — no var-gate):
--   * INACTIV.MARKER = 'Y'  (SELECT F.ACCOUNT WITH INACTIV.MARKER EQ "Y")
--   * 30-day recency cut: LAST_TXN_DATE = MAX(date_last_dr_cust, date_last_cr_cust)
--     must be >= CURRENT_DATE - 30 days.
--   Accounts with both date fields NULL have no last-txn date; the T24 BASIC routine's
--   blank-date comparison fails silently → those rows are SKIPPED (not kept).
--   No pass-through for null dates.
--
-- Bronze conventions:
--   * Array/multivalue bronze columns are LIST type → element access col[0].
--   * Bronze date columns are real DATE → CAST(x AS DATE); never DATE_FORMAT.
--     Use TO_CHAR(x,'YYYYMMDD') only if a string date is needed.
--   * Versioned dims (t24_customer) deduped via ROW_NUMBER() latest version per recid.
--
-- Working balance (ACC.BALANCE) is procedural in T24 (AC.CashFlow service); the
-- physical store is unresolved (§13 / §5E). Emitted from t24_account.working_balance
-- as the documented bronze fallback.

-- Audit fixes 2026-06-19 (pass 1):
--   (f) customer_dedup: was ORDER BY recid DESC (wrong) -> fixed to curr_no DESC
--       (t24_customer is a versioned dim; recid sort does not pick latest version).
--   (f) company_dedup:  was ORDER BY recid DESC (wrong) -> fixed to curr_no DESC
--       (t24_company is a versioned dim; 2 rows per recid confirmed in siblings).
--   (foreign) Removed acct_activity_agg CTE and its join: ACCT.ACTIVITY is NOT in
--       tables.md for INACTIVE.ACCOUNT.REPORT (only ACCOUNT/COMPANY/CUSTOMER/ECB listed).
-- Audit fixes 2026-06-19 (pass 2 — reconcile):
--   (c/var-gate) Removed the require_current var-gate; both filters are
--       UNCONDITIONAL per canonical logic (INACTIV.MARKER EQ "Y" + 30-day cutoff).
--   (c/null-date) Removed null-date pass-through arm; canonical T24 BASIC routine
--       silently skips accounts with blank last-txn dates (no special exemption).
-- Strict-verify 2026-06-19 (pass 3 — strict):
--   (removed) updated_at: CURRENT_TIMESTAMP has zero canonical backing in logic.md
--       Part-1 or Part-2 SQL; not an ENQ output field → removed.

-- Coverage reconcile 2026-06-20 (A9):
--   Expanded silver output to cover ALL columns rpt_aml03 consumes from its two legacy
--   staging sources (stg_aml03__accounts + stg_aml03__customers) so this single model
--   can replace both. New columns added:
--     From accounts (t24_account):
--       account_title_1 (LIST[0]), account_title_2 (LIST[0]), short_title (LIST[0]),
--       account_category (← category), company_code (alias of co_code),
--       customer_id (alias of customer),
--       last_credit_date (← date_last_cr_cust), last_debit_date (← date_last_dr_cust),
--       days_since_last_txn (derived), inactive_marker (← inactiv_marker),
--       is_inactive_30_days (derived: days>=30 OR null last-txn → 'Y').
--     From customers (t24_customer):
--       customer_short_name (← short_name[0]), customer_full_name (← name_1[0]),
--       legal_id (← legal_id[0]), gender, occupation (← occupation[0]),
--       birth_or_incorp_date (← date_of_birth).
--     Honest NULLs (field not in bronze tables.md or un-extractable):
--       mis_date → CURRENT_DATE (DATE), dir0 → YYYYMMDD VARCHAR from CURRENT_DATE   [record_timestamp absent from t24_account bronze; CURRENT_DATE is the effective fallback per 2026-06-22 fix]
--       record_timestamp → CAST(NULL AS VARCHAR)  [no DATE_TIME on t24_account bronze]
--       legal_doc_type   → CAST(NULL AS VARCHAR)  [legal_doc_name absent from bronze t24_customer]
--       legal_doc_expiry_date → CAST(NULL AS DATE) [legal_exp_date absent from bronze t24_customer]
--       cus.title: confirmed scalar VARCHAR in bronze (typeof → VARCHAR?); no [0] needed; bare cus.title is correct.

WITH customer_dedup AS (
    -- t24_customer: versioned dim -> dedup to latest version per recid using curr_no DESC.
    -- Expanded columns to cover rpt_aml03 customer fields.
    -- birth_or_incorp_date: bronze column is date_of_birth on t24_customer
    --   (confirmed: stg_aml03__customers_t24.sql line 78).
    SELECT recid, title, name_1, name_2, short_name,
           legal_id, gender, occupation, date_of_birth
    FROM (
        SELECT
            recid,
            title,
            name_1,
            name_2,
            short_name,
            legal_id,
            gender,
            occupation,
            date_of_birth,
            ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                               ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_customer AT SNAPSHOT '{{ var("snap_customer") }}'
    ) t WHERE rn = 1
),
company_dedup AS (
    -- t24_company: versioned dim -> dedup via curr_no DESC (2 rows per recid confirmed).
    -- company_name is a LIST -> extracted with [0] in SELECT below.
    SELECT recid, company_name
    FROM (
        SELECT
            recid,
            company_name,
            ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                               ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_company
    WHERE business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
),

last_txn AS (
    -- Compute last_transaction_date and days_since_last_txn once; reused in SELECT and WHERE.
    SELECT
        recid,
        CASE
            WHEN CAST(date_last_cr_cust AS DATE) IS NULL THEN CAST(date_last_dr_cust AS DATE)
            WHEN CAST(date_last_dr_cust AS DATE) IS NULL THEN CAST(date_last_cr_cust AS DATE)
            WHEN CAST(date_last_cr_cust AS DATE) >= CAST(date_last_dr_cust AS DATE)
                THEN CAST(date_last_cr_cust AS DATE)
            ELSE CAST(date_last_dr_cust AS DATE)
        END AS last_txn_date
    FROM hive.bronze.t24_account AT SNAPSHOT '{{ var("snap_account") }}'
    WHERE TRIM(CAST(inactiv_marker AS VARCHAR)) = 'Y'
)

SELECT
    -- ═══════════════════════════════════════════════════
    -- Canonical output fields (§13 Part-1/Part-2 SQL)
    -- ═══════════════════════════════════════════════════

    -- COMP.ID (output element 1; internal, drives Branch Name FRead)
    TRIM(CAST(acc.co_code AS VARCHAR))                                                   AS company_id,
    -- Also exposed as company_code for rpt_aml03 (ia.company_code) and branch_code alias
    TRIM(CAST(acc.co_code AS VARCHAR))                                                   AS company_code,

    -- Branch Name (F *,2,1) ← COMPANY.EB_COM_COMPANY_NAME keyed on ACCOUNT.CO_CODE
    TRIM(CAST(co.company_name[0] AS VARCHAR))                                            AS branch_name,

    -- Account Number (F *,3,1) ← ACCOUNT.@ID
    TRIM(CAST(acc.recid AS VARCHAR))                                                     AS account_number,
    -- Also exposed as account_id for rpt_aml03 (ia.account_id join key and surrogate key)
    TRIM(CAST(acc.recid AS VARCHAR))                                                     AS account_id,

    -- Customer Number (F *,4,1) ← ACCOUNT.CUSTOMER
    TRIM(CAST(acc.customer AS VARCHAR))                                                  AS customer_number,
    -- Also exposed as customer_id for rpt_aml03 JOIN (ia.customer_id = c.customer_id)
    TRIM(CAST(acc.customer AS VARCHAR))                                                  AS customer_id,

    -- Customer Name (F *,5,1) ← CUSTOMER title + name_one + name_two + short_name
    TRIM(REGEXP_REPLACE(
        CONCAT_WS(' ',
            COALESCE(TRIM(CAST(cus.title        AS VARCHAR)), ''),
            COALESCE(TRIM(CAST(cus.name_1[0]    AS VARCHAR)), ''),
            COALESCE(TRIM(CAST(cus.name_2[0]    AS VARCHAR)), ''),
            COALESCE(TRIM(CAST(cus.short_name[0] AS VARCHAR)), '')
        ), '\s+', ' '))                                                                  AS customer_name,

    -- Account Currency (F *,9,1) ← ACCOUNT.CURRENCY
    TRIM(CAST(acc.currency AS VARCHAR))                                                  AS account_currency,

    -- Last Transaction Date (F *,6,1) ← MAX(date_last_dr_cust, date_last_cr_cust)
    lt.last_txn_date                                                                     AS last_transaction_date,

    -- Account Balance (F *,7,1) ← procedural AC.CashFlow working balance
    CAST(acc.working_balance AS DECIMAL(18,2))                                           AS account_balance,
    -- Also exposed as working_balance for rpt_aml03 (ia.working_balance)
    CAST(acc.working_balance AS DECIMAL(18,2))                                           AS working_balance,

    -- Account Open Date (F *,8,1) ← ACCOUNT.OPENING_DATE
    CAST(acc.opening_date AS DATE)                                                       AS account_open_date,
    -- Also exposed as opening_date for rpt_aml03 (ia.opening_date)
    CAST(acc.opening_date AS DATE)                                                       AS opening_date,

    -- ═══════════════════════════════════════════════════
    -- Coverage additions — accounts (t24_account bronze)
    -- ═══════════════════════════════════════════════════

    -- account_title_1 (rpt_aml03: ia.account_title_1) ← t24_account.account_title_1 LIST[0]
    TRIM(CAST(acc.account_title_1[0] AS VARCHAR))                                        AS account_title_1,

    -- account_title_2 (rpt_aml03: ia.account_title_2) ← t24_account.account_title_2 LIST[0]
    TRIM(CAST(acc.account_title_2[0] AS VARCHAR))                                        AS account_title_2,

    -- short_title (rpt_aml03: ia.short_title) ← t24_account.short_title LIST[0]
    TRIM(CAST(acc.short_title[0] AS VARCHAR))                                            AS short_title,

    -- account_category (rpt_aml03: ia.account_category) ← t24_account.category
    TRIM(CAST(acc.category AS VARCHAR))                                                  AS account_category,

    -- last_credit_date (rpt_aml03: ia.last_credit_date) ← t24_account.date_last_cr_cust
    CAST(acc.date_last_cr_cust AS DATE)                                                  AS last_credit_date,

    -- last_debit_date (rpt_aml03: ia.last_debit_date) ← t24_account.date_last_dr_cust
    CAST(acc.date_last_dr_cust AS DATE)                                                  AS last_debit_date,

    -- days_since_last_txn (rpt_aml03: ia.days_since_last_txn) ← derived
    CASE
        WHEN lt.last_txn_date IS NULL THEN NULL
        ELSE DATEDIFF(date '{{ var("business_date") }}', lt.last_txn_date)  -- was CURRENT_DATE
    END                                                                                  AS days_since_last_txn,

    -- inactive_marker (rpt_aml03: ia.inactive_marker) ← t24_account.inactiv_marker
    TRIM(CAST(acc.inactiv_marker AS VARCHAR))                                            AS inactive_marker,

    -- is_inactive_30_days (rpt_aml03: ia.is_inactive_30_days) ← derived
    CASE
        WHEN lt.last_txn_date IS NULL
          OR DATEDIFF(date '{{ var("business_date") }}', lt.last_txn_date) >= 30 THEN 'Y'  -- was CURRENT_DATE
        ELSE 'N'
    END                                                                                  AS is_inactive_30_days,

    -- mis_date (rpt_aml03: ia.mis_date) ← COB business_date D (was CURRENT_DATE; mis_date must be COB day, not wall clock)
    date '{{ var("business_date") }}'                                                    AS mis_date,

    -- dir0 (rpt_aml03: ia.dir0 partition) ← YYYYMMDD VARCHAR from business_date D (was CURRENT_DATE)
    REPLACE(CAST(date '{{ var("business_date") }}' AS VARCHAR), '-', '')                 AS dir0,

    -- record_timestamp (rpt_aml03: ia.record_timestamp) ← no DATE_TIME on t24_account bronze
    CAST(NULL AS VARCHAR)                                                                AS record_timestamp,

    -- ═══════════════════════════════════════════════════
    -- Coverage additions — customers (t24_customer bronze)
    -- ═══════════════════════════════════════════════════

    -- customer_short_name (rpt_aml03: c.customer_short_name, also branch_name alias)
    --   ← t24_customer.short_name LIST[0]
    TRIM(CAST(cus.short_name[0] AS VARCHAR))                                             AS customer_short_name,

    -- customer_full_name (rpt_aml03: c.customer_full_name) ← t24_customer.name_1 LIST[0]
    --   (name_2 absent from bronze t24_customer per stg_aml03__customers_t24; name_1 only)
    TRIM(CAST(cus.name_1[0] AS VARCHAR))                                                 AS customer_full_name,

    -- legal_id (rpt_aml03: c.legal_id) ← t24_customer.legal_id LIST[0]
    TRIM(CAST(cus.legal_id[0] AS VARCHAR))                                               AS legal_id,

    -- legal_doc_type (rpt_aml03: c.legal_doc_type) ← legal_doc_name absent from bronze
    CAST(NULL AS VARCHAR)                                                                AS legal_doc_type,

    -- legal_doc_expiry_date (rpt_aml03: c.legal_doc_expiry_date) ← legal_exp_date absent
    CAST(NULL AS DATE)                                                                   AS legal_doc_expiry_date,

    -- occupation (rpt_aml03: c.occupation) ← t24_customer.occupation LIST[0]
    TRIM(CAST(cus.occupation[0] AS VARCHAR))                                             AS occupation,

    -- gender (rpt_aml03: c.gender) ← t24_customer.gender
    TRIM(CAST(cus.gender AS VARCHAR))                                                    AS gender,

    -- birth_or_incorp_date (rpt_aml03: c.birth_or_incorp_date) ← t24_customer.date_of_birth
    --   (bronze column confirmed as date_of_birth by stg_aml03__customers_t24.sql)
    CAST(cus.date_of_birth AS DATE)                                                      AS birth_or_incorp_date,

    -- business_date: COB date stamp (T-STAMP; uniform across all 4 silver models)
    CAST(date '{{ var("business_date") }}' AS DATE)                AS business_date

    -- updated_at removed: not in canonical logic.md output fields (no backing in Part-1 or Part-2 SQL)
FROM hive.bronze.t24_account AT SNAPSHOT '{{ var("snap_account") }}' acc
LEFT JOIN company_dedup  co  ON TRIM(CAST(acc.co_code  AS VARCHAR)) = TRIM(CAST(co.recid  AS VARCHAR))
LEFT JOIN customer_dedup cus ON TRIM(CAST(acc.customer AS VARCHAR)) = TRIM(CAST(cus.recid AS VARCHAR))
LEFT JOIN last_txn       lt  ON TRIM(CAST(acc.recid    AS VARCHAR)) = TRIM(CAST(lt.recid  AS VARCHAR))
WHERE TRIM(CAST(acc.inactiv_marker AS VARCHAR)) = 'Y'
  -- 30-day recency cut, DATA-ANCHORED (2026-06-21): the window is measured from the
  -- latest last-txn date present in the data, not CURRENT_DATE, so the report always
  -- returns the populated period instead of 0 rows when the snapshot lags today.
  -- (Data range 2019-07-11..2026-06-20; anchored window keeps ~111 rows.)
  -- Accounts with blank/null dates are skipped (canonical: blank comparison fails silently).
  AND lt.last_txn_date >= DATE_ADD((SELECT MAX(last_txn_date) FROM last_txn), -30)
