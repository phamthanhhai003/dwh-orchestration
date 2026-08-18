{{ config(
    materialized='table',
    database='hive',
    schema='silver'
) }}

-- Division: credit | Report: §8 E.ACCOUNT.OPEN (t24-built BRONZE source table)
-- BRONZE-zone t24-built equivalent of the AML daily accounts-opened report.
-- Emits the FULL Part-1 §8 E.ACCOUNT.OPEN field set (18 §-fields) from real
-- hive.bronze.t24_* tables, per T24_REPORT_DEVELOPMENT_SOT.md "### 8. E.ACCOUNT.OPEN".
--
-- Grain: one row per ACCOUNT opened on <target_date>
--   (driver substitution for ACCOUNT.CONCAT: t24_account WHERE opening_date = <target_date>).
--
-- Bronze conventions (per reference model stg__loan_sector_detail_t24.sql):
--   * LIST/multivalue bronze columns are LIST type -> element access col[0] (NOT "col/0").
--     legal_id / legal_doc_name on t24_customer are LIST -> [0]/[1].
--     occupation on t24_customer is an ARRAY -> occupation[0].
--   * Versioned dims (t24_customer) carry multiple rows per recid -> dedup latest version
--     per recid (ROW_NUMBER ... ORDER BY curr_no DESC) so the join stays 1:1.
--   * Bronze date columns are real DATE -> CAST(x AS DATE). Never DATE_FORMAT.
--   * The active-row filter (opening_date = <target_date>) is UNCONDITIONAL per
--     canonical selection (var-gate removed; reconciled 2026-06-19).

WITH
-- Dimension dedup: bronze t24_customer carries multiple versioned rows per recid.
-- Keep latest version per recid (drop business_date for customer per convention) so the
-- 1-row-per-account grain is preserved.
customer_dedup AS (
    SELECT
        recid,
        title,
        name_1,
        name_2,
        short_name,
        legal_id,
        legal_doc_name,
        pep,
        birth_incorp_date,
        date_of_birth,
        nationality,
        cust_birth_country,
        street,
        occupation
    FROM (
        SELECT
            recid, title, name_1, name_2, short_name, legal_id, legal_doc_name,
            pep, birth_incorp_date, date_of_birth, nationality, cust_birth_country,
            street, occupation,
            ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                               ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_customer AT SNAPSHOT '{{ var("snap_customer") }}'
    ) t WHERE rn = 1
),
-- COMPANY param dedup (branch name lookup), keyed on company id (= account.co_code).
company_dedup AS (
    SELECT recid, company_name FROM (
        SELECT recid, company_name,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_company
    WHERE business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
),
-- First-deposit lookup (§8 field First.Deposit.Amt): first positive-amount entry per account.
-- Canonical path (logic.md §9-12):
--   a) DETAIL path: netted entries (recid LIKE '%!%') resolve via STMT.ENTRY.DETAIL.XREF
--      (xr.recid = se.recid) → STMT.ENTRY.DETAIL (sed.recid = xr.detail_id); use sed.amount_lcy.
--   b) NORMAL path: plain entries (recid NOT LIKE '%!%') read directly from STMT.ENTRY; use se.amount_lcy.
-- Both paths unioned; earliest positive-amount entry (by booking_date ASC) wins.
-- Join keys mirror sibling model t24_cui_stmt01.sql (confirmed bronze column names).
-- NOTE: EB.ACCT.ENTRY.LIST date-window filter (FROM.DATE=OpeningDate, TO.DATE=today) cannot be
-- reconstructed without live schema proof that t24_stmt_entry carries a USABLE window column;
-- the full account history is scanned and earliest positive entry is taken as faithful approximation.
-- first_deposit_amt is a REAL value sourced from bronze when a matching entry exists; NULL otherwise.
first_deposit_detail AS (
    -- Netted entries: STMT.ENTRY.DETAIL via STMT.ENTRY.DETAIL.XREF
    SELECT
        TRIM(CAST(se.account_number AS VARCHAR))                    AS account_number,
        CAST(sed.amount_lcy AS DECIMAL(18,2))                       AS amount_lcy,
        CAST(sed.booking_date AS DATE)                              AS booking_date
    FROM hive.bronze.t24_stmt_entry AT SNAPSHOT '{{ var("snap_stmt_entry") }}' se
    JOIN hive.bronze.t24_stmt_entry_detail_xref AT SNAPSHOT '{{ var("snap_stmt_entry_detail_xref") }}' xr
        ON TRIM(CAST(se.recid AS VARCHAR)) = TRIM(CAST(xr.recid AS VARCHAR))
    JOIN hive.bronze.t24_stmt_entry_detail AT SNAPSHOT '{{ var("snap_stmt_entry_detail") }}' sed
        ON TRIM(CAST(xr.detail_id AS VARCHAR)) = TRIM(CAST(sed.recid AS VARCHAR))
    WHERE CAST(se.recid AS VARCHAR) LIKE '%!%'
      AND CAST(sed.amount_lcy AS DECIMAL(18,2)) > 0
),
first_deposit_plain AS (
    -- Plain (non-netted) entries: read directly from STMT.ENTRY
    SELECT
        TRIM(CAST(account_number AS VARCHAR))                       AS account_number,
        CAST(amount_lcy AS DECIMAL(18,2))                          AS amount_lcy,
        CAST(booking_date AS DATE)                                  AS booking_date
    FROM hive.bronze.t24_stmt_entry AT SNAPSHOT '{{ var("snap_stmt_entry") }}'
    WHERE CAST(recid AS VARCHAR) NOT LIKE '%!%'
      AND CAST(amount_lcy AS DECIMAL(18,2)) > 0
),
first_deposit AS (
    -- Union both paths; pick the earliest positive entry per account (logic.md §11-12 priority:
    -- DETAIL path is tried first in the routine, but both are in order by booking_date so unioning
    -- and taking rn=1 by booking_date faithfully implements the "first positive entry" rule).
    SELECT account_number, amount_lcy FROM (
        SELECT
            account_number,
            amount_lcy,
            booking_date,
            ROW_NUMBER() OVER (
                PARTITION BY account_number
                ORDER BY booking_date ASC
            )                                                       AS rn
        FROM (
            SELECT account_number, amount_lcy, booking_date FROM first_deposit_detail
            UNION ALL
            SELECT account_number, amount_lcy, booking_date FROM first_deposit_plain
        ) combined
    ) t WHERE rn = 1
)

SELECT
    -- ===== §8 E.ACCOUNT.OPEN full field set =====
    TRIM(CAST(acc.co_code AS VARCHAR))                                                  AS branch_no,
    TRIM(CAST(co.company_name[0] AS VARCHAR))                                           AS branch_name,
    TRIM(CAST(acc.recid AS VARCHAR))                                                    AS acct_no,
    TRIM(CAST(acc.customer AS VARCHAR))                                                 AS cus_no,
    LTRIM(RTRIM(CONCAT_WS(' ',
        TRIM(CAST(cus.title AS VARCHAR)),
        TRIM(CAST(cus.name_1[0] AS VARCHAR)),
        TRIM(CAST(cus.name_2[0] AS VARCHAR)),
        TRIM(CAST(cus.short_name[0] AS VARCHAR)))))                                     AS cus_name,
    CAST(acc.opening_date AS DATE)                                                      AS open_acct_date,
    TRIM(CAST(luk1.lu_desc AS VARCHAR))                                                 AS ident_doc_type,
    TRIM(CAST(cus.legal_id[0] AS VARCHAR))                                              AS ident_doc_no,
    TRIM(CAST(luk2.lu_desc AS VARCHAR))                                                  AS alter_id_type,
    TRIM(CAST(cus.legal_id[1] AS VARCHAR))                                              AS alter_id_no,
    TRIM(CAST(cus.pep AS VARCHAR))                                                      AS pep,
    CAST(COALESCE(cus.birth_incorp_date, cus.date_of_birth) AS DATE)                    AS dob,
    TRIM(CAST(cus.nationality AS VARCHAR))                                              AS nationality,
    TRIM(CAST(cus.cust_birth_country AS VARCHAR))                                       AS birth_country,
    TRIM(CAST(cus.street[0] AS VARCHAR))                                                AS address,
    TRIM(CAST(cus.occupation[0] AS VARCHAR))                                            AS occupation,
    CAST(acc.working_balance AS DECIMAL(18,2))                                          AS acct_bal,
    fd.amount_lcy                                                                       AS first_deposit_amt,
    -- ===== Additional fields from current source hive.bronze."aml_aml03" (coverage reconcile 2026-06-20) =====
    -- account_ccy: Account currency code (ACCOUNT.Currency in ACC.TODAY; ACCOUNT table, tables.md).
    TRIM(CAST(acc.currency AS VARCHAR))                                                 AS account_ccy,
    -- last_txn_date: Last transaction date = later of DATE.LAST.CR.CUST / DATE.LAST.DR.CUST
    --   (max of credit/debit date per ACCOUNT; same derivation as stg_aml03__accounts_t24 §13).
    CAST(
        CASE
            WHEN CAST(acc.date_last_cr_cust AS DATE) IS NULL THEN CAST(acc.date_last_dr_cust AS DATE)
            WHEN CAST(acc.date_last_dr_cust AS DATE) IS NULL THEN CAST(acc.date_last_cr_cust AS DATE)
            WHEN CAST(acc.date_last_cr_cust AS DATE) >= CAST(acc.date_last_dr_cust AS DATE)
                THEN CAST(acc.date_last_cr_cust AS DATE)
            ELSE CAST(acc.date_last_dr_cust AS DATE)
        END
    AS DATE)                                                                            AS last_txn_date,
    -- process_date: Parser/ETL snapshot date; no equivalent bronze column -> CAST NULL.
    CAST(NULL AS DATE)                                                                  AS process_date,
    -- business_date: COB date stamp (T-STAMP; uniform across all 4 silver models)
    CAST(date '{{ var("business_date") }}' AS DATE)                AS business_date
    -- gic_doc_type REMOVED: GIC.ID is dead/no-op in ACC.TODAY (ALT.ID.TYP never assigned; result never used); not in canonical 18-field output (logic.md + tables.md).
FROM hive.bronze.t24_account AT SNAPSHOT '{{ var("snap_account") }}' acc
LEFT JOIN company_dedup  co   ON TRIM(CAST(acc.co_code AS VARCHAR))   = TRIM(CAST(co.recid AS VARCHAR))
LEFT JOIN customer_dedup cus  ON TRIM(CAST(acc.customer AS VARCHAR))  = TRIM(CAST(cus.recid AS VARCHAR))
LEFT JOIN first_deposit fd    ON fd.account_number = TRIM(CAST(acc.recid AS VARCHAR))
-- §8 id-doc-type description: EB.LOOKUP keyed 'CUS.LEGAL.DOC.NAME*'+legal_doc_name[0] (primary doc)
-- Dedup: bronze t24_eb_lookup carries multiple versioned rows per recid (curr_no); keep latest
-- per recid (ROW_NUMBER ... curr_no DESC) so this lookup join stays 1:1 and does NOT fan out the
-- account grain (reconciled 2026-06-22: 3 identical curr_no versions per recid inflated rows ×3).
LEFT JOIN (
    SELECT recid, lu_desc FROM (
        SELECT recid, description[0] AS lu_desc,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR)) ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_eb_lookup WHERE recid LIKE 'CUS.LEGAL.DOC.NAME%' AND business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
) luk1
    ON TRIM(CAST(luk1.recid AS VARCHAR)) = CONCAT('CUS.LEGAL.DOC.NAME*', TRIM(CAST(cus.legal_doc_name[0] AS VARCHAR)))
-- §8 alter-id-doc-type description: EB.LOOKUP keyed 'CUS.LEGAL.DOC.NAME*'+legal_doc_name[1] (alternate doc)
LEFT JOIN (
    SELECT recid, lu_desc FROM (
        SELECT recid, description[0] AS lu_desc,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR)) ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_eb_lookup WHERE recid LIKE 'CUS.LEGAL.DOC.NAME%' AND business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
) luk2
    ON TRIM(CAST(luk2.recid AS VARCHAR)) = CONCAT('CUS.LEGAL.DOC.NAME*', TRIM(CAST(cus.legal_doc_name[1] AS VARCHAR)))
-- §8 selection: accounts opened on the user-supplied date (ACCOUNT.CONCAT substitute).
-- target_date empty -> ALL opening dates materialize (full-refresh all-dates mode, 2026-06-22);
-- target_date set -> single business date (incremental/date mode).
WHERE ('{{ var("target_date", "") }}' = ''
       OR CAST(acc.opening_date AS DATE) = CAST(NULLIF('{{ var("target_date", "") }}', '') AS DATE))
