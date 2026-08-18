{{ config(
    materialized='table',
    database='hive',
    schema='silver'
) }}

-- Division: credit | Object: NOFILE.BNCTL.DAILY.TXN.9k (t24-built source, SILVER zone)
-- Reads real hive.bronze.t24_* per NOFILE.BNCTL.DAILY.TXN.9k canonical logic
-- (T24_sourcing_doc/T24-sourcing-output/objects/NOFILE.BNCTL.DAILY.TXN.9k/logic.md + tables.md).
--
-- What it produces: Daily Large Transaction (>=9K) AML/CTR-style listing.
-- USD accounts whose same-day net absolute statement movement reaches >= 9,000.
-- One row per STMT.ENTRY for every qualifying account.
--
-- Grain: one row per STMT.ENTRY record, for accounts whose SUM(ABS(amount_lcy))
--   across today's entries >= 9000 AND whose last entry currency = 'USD'.
--
-- Tables used (per tables.md):
--   STMT.ENTRY         -> hive.bronze.t24_stmt_entry             (core driver)
--   STMT.ENTRY.DETAIL  -> absent from bronze; t24_stmt_entry covers live+archived (gap, documented)
--   ACCOUNT            -> hive.bronze.t24_account AT SNAPSHOT '{{ var("snap_account") }}'                (core; 1 row/recid, no dedup needed)
--   CUSTOMER           -> hive.bronze.t24_customer AT SNAPSHOT '{{ var("snap_customer") }}'               (enrichment; versioned, dedup curr_no DESC)
--   TRANSACTION        -> hive.bronze.t24_transaction            (param; versioned, dedup curr_no DESC)
--   TELLER             -> hive.bronze.t24_teller AT SNAPSHOT '{{ var("snap_teller") }}'                 (enrichment; TT entries w/ blank narrative)
--   EB.LOOKUP          -> hive.bronze.t24_eb_lookup              (param; legal doc type description)
--   GIC.ID             -> hive.bronze.t24_gic_id                 (param; fallback legal doc description)
--   TODAY.TXN.ACC      -> runtime flat file; substituted by t24_stmt_entry WHERE booking_date = <date>
--   ACCT.ENT.TODAY     -> access handle; same substitution as TODAY.TXN.ACC (no join)
--   ACCT.ACTIVITY      -> AC.AVG.BAL routine; sourced from t24_acct_activity (FBNK_ACCT_ACTIVITY);
--                         recid = '<account>-<YYYYMM>'; deduped to latest month per account.
--                         avg_balance = balance[0] (latest month day-balance[0]); best-available
--                         approximation — AC.AVG.BAL day-weighted formula not reproducible in SQL.
--   ACCT.ENT.TODAY     -> access handle (FBNK_ACCT_ENT_TODAY); sourced from t24_acct_ent_today;
--                         keyed by account_number (recid = today account-entries key);
--                         emits entry_no (today-entries count/reference handle).
--   COMPANY.CONSOL     -> speculation/vestigial INIT open (F_COMPANY_CONSOL, tables.md);
--                         sourced from t24_company_consol; keyed on co_code; versioned (curr_no DESC);
--                         emits com_consol_to (consolidated-to company). No actual read in main loop.
--
-- Tables added (wired in this model):
--   t24_acct_activity   — FBNK_ACCT_ACTIVITY; deduped by account prefix (recid LIKE '<acct>-<YYYYMM>'),
--                         latest month; contributes avg_balance (replaces prior CAST(NULL...) stub).
--   t24_acct_ent_today  — FBNK_ACCT_ENT_TODAY; access handle; keyed by account_number; contributes entry_no.
--   t24_company_consol  — F_COMPANY_CONSOL; vestigial/speculation; keyed on co_code (versioned, curr_no DESC);
--                         contributes com_consol_to.
-- Tables NOT in tables.md (removed):
--   t24_funds_transfer  — foreign table, zero projected columns
--   t24_company         — COMPANY.CONSOL is vestigial; co_code taken directly from t24_account
--
-- Teller join: applied only for entries with system_id = 'TT' AND blank narrative.
--   Teller @ID = piece before '\' in trans_reference (piece before ';').
--   Used ONLY for narrative override (teller_id is NOT a canonical output column).
--
-- 9K threshold (canonical): account-level gate. SUM(ABS(amount_lcy)) over all today-entries
--   per account >= 9000, AND last entry's currency = 'USD'. Applied via window function so
--   the entire entry block for a qualifying account is included, not just entries >= 9000.
--
-- Bronze conventions (C1 pilot):
--   * Array/multivalue bronze columns are LIST type -> element access col[0] (NOT "col/0").
--     occupation, legal_id, legal_doc_name, name_1, name_2, short_name on t24_customer are LIST -> [0].
--     account_title_1, short_title on t24_account are LIST -> [0].
--   * Versioned dim tables carry >1 row per recid -> dedup latest via ROW_NUMBER() curr_no DESC.
--     t24_customer, t24_transaction are versioned dims.
--     t24_account is 1 row/recid (no dedup CTE required).
--   * Bronze date columns are real DATE -> CAST(x AS DATE). Never DATE_FORMAT.
--   * Filters (booking_date gate and 9K threshold) are unconditional per canonical (var-gate removed).
--
-- Derived-via-routine gaps (partially resolved):
--   * avg_balance     -> AC.AVG.BAL routine (ACCT.ACTIVITY day-balances); now sourced from
--                        t24_acct_activity.balance[0] (latest month; approximation — day-weighted
--                        formula is procedural and not reproducible in SQL). See acct_activity_dedup CTE.
--   * working_balance -> AC.CashFlow.AccountServiceGetWorkingBalance; t24_account.working_balance
--                        is a static snapshot balance, used here as best available approximation.
--   * STMT.ENTRY.DETAIL fallback -> not a separate bronze table; t24_stmt_entry assumed to merge
--                        live and archived entries. No COALESCE fallback implemented (gap, documented).
--   * account_title_2 -> not in t24_account bronze scope; NULL.

WITH customer_dedup AS (
    -- t24_customer: versioned dim -> dedup latest per recid via curr_no DESC.
    -- Expose all canonical CUSTOMER fields: name, legal id, doc type, occupation,
    -- plus coverage fields: gender, date_of_birth needed by rpt_9k_transaction.
    SELECT recid, title, name_1, name_2, short_name,
           legal_id, legal_doc_name, occupation,
           gender, date_of_birth
    FROM (
        SELECT recid, title, name_1, name_2, short_name,
               legal_id, legal_doc_name, occupation,
               gender, date_of_birth,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_customer AT SNAPSHOT '{{ var("snap_customer") }}'
    ) t WHERE rn = 1
),
transaction_dedup AS (
    -- t24_transaction: versioned dim -> dedup latest per recid via curr_no DESC.
    -- stmt_narr = TRANSACTION narrative/description for the entry's transaction code.
    SELECT recid, stmt_narr FROM (
        SELECT recid, stmt_narr,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_transaction
    WHERE business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
),
-- COMPANY branch lookup (versioned dim -> dedup latest per recid via curr_no DESC).
-- Fix (2026-06-23): branch_name was a hardcoded co_code->name CASE fed by acc.co_code, but the
-- ACCOUNT join is fully broken (t24_account is a different data vintage: 0 of 24,720 distinct
-- stmt_entry.account_number keys match t24_account.recid). Branch is now sourced from a real
-- COMPANY lookup keyed on t24_stmt_entry.company_code (the entry's own company, 100,240/100,342
-- populated, 14 distinct = the 14 branches), independent of the broken ACCOUNT join.
-- recid = co_code (TL00100xx); mnemonic = canonical branch short-name (BNK/DIL/GLN/...).
company_dedup AS (
    SELECT recid, mnemonic FROM (
        SELECT recid, mnemonic,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_company
    WHERE business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
),
-- §NOFILE.BNCTL.DAILY.TXN.9k §5h: ACCT.ACTIVITY (FBNK_ACCT_ACTIVITY) — AC.AVG.BAL source.
-- recid format: '<account_no>-<YYYYMM>' (one row per account-month).
-- Dedup: extract account prefix (SPLIT_PART(recid,'-',1)), keep latest month (recid DESC = latest YYYYMM).
-- avg_balance sourced from balance[0] (first day-balance in the MV array for the latest month).
-- Note: AC.AVG.BAL full formula (SUM(day_balance*num_days)/cr_days|dr_days, currency-rounded)
-- is procedural and not reproducible in SQL; balance[0] is the best-available static approximation.
-- logic.md §Part1/step5h: "EbReadAcctActivityRecord(ACCID='<acct>-<YYYYMM>') ... walks each day's
-- day-balance, sums balance×days over the window and divides by debit/credit day counts."
acct_activity_dedup AS (
    SELECT
        SPLIT_PART(TRIM(CAST(recid AS VARCHAR)), '-', 1)  AS account_no,
        balance
    FROM (
        SELECT recid, balance,
               ROW_NUMBER() OVER (
                   PARTITION BY SPLIT_PART(TRIM(CAST(recid AS VARCHAR)), '-', 1)
                   ORDER BY TRIM(CAST(recid AS VARCHAR)) DESC
               ) AS rn
        FROM hive.bronze.t24_acct_activity AT SNAPSHOT '{{ var("snap_acct_activity") }}'
    ) t WHERE rn = 1
),
-- §NOFILE.BNCTL.DAILY.TXN.9k driver: TODAY.TXN.ACC flat file substituted by
-- t24_stmt_entry WHERE booking_date = <report_date>. The 3-digit prefix guard
-- (account_id LIKE '3-digit prefix') is implicit in the booking_date filter.
-- Date gate: when target_date is supplied, filter to that date (incremental / Mode B).
--            when target_date is empty (full-refresh / Mode A), return ALL dates.
-- Fix (2026-06-22): CAST(NULLIF('','')/NULL) = NULL → 0 rows in full-refresh; replaced
-- with explicit empty-string guard so Mode A materialises all bronze dates.
entries AS (
    -- T-EVENT: stmt_entry is an event table read LIVE (no AT SNAPSHOT); driver keyed on
    -- booking_date = COB business_date D (was target_date var). One COB day per run.
    SELECT * FROM hive.bronze.t24_stmt_entry
    WHERE CAST(booking_date AS DATE) = date '{{ var("business_date") }}'
),
-- Window-aggregate per account: total abs amount and entry count (canonical ENTRY.CNT)
-- and last-entry currency (canonical currency gate = last Y.CCY in the account's loop).
-- Used to apply the account-level >= 9000 + USD filter in the final WHERE.
detail AS (
    SELECT
        se.*,
        -- Canonical dr/cr split: AMOUNT.LCY starts with '-' -> debit, else credit
        CASE WHEN LEFT(TRIM(CAST(se.amount_lcy AS VARCHAR)), 1) = '-'
             THEN ABS(CAST(se.amount_lcy AS DECIMAL(18,2)))
             ELSE CAST(0 AS DECIMAL(18,2))
        END                                                                        AS dr_amount,
        CASE WHEN LEFT(TRIM(CAST(se.amount_lcy AS VARCHAR)), 1) != '-'
             THEN CAST(se.amount_lcy AS DECIMAL(18,2))
             ELSE CAST(0 AS DECIMAL(18,2))
        END                                                                        AS cr_amount,
        -- Account-level running total of absolute amounts (canonical Y.TOTAL.AMT)
        SUM(ABS(CAST(se.amount_lcy AS DECIMAL(18,2)))) OVER (
            PARTITION BY TRIM(CAST(se.account_number AS VARCHAR))
        )                                                                          AS acct_total,
        -- Entry count per account (canonical ENTRY.CNT)
        COUNT(*) OVER (
            PARTITION BY TRIM(CAST(se.account_number AS VARCHAR))
        )                                                                          AS entry_cnt,
        -- Last entry's currency for the account (canonical Y.CCY gate on USD)
        LAST_VALUE(se.currency) OVER (
            PARTITION BY TRIM(CAST(se.account_number AS VARCHAR))
            ORDER BY se.recid
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                                                          AS last_ccy,
        -- Row sequence within account (for ORDER BY stability)
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(CAST(se.account_number AS VARCHAR))
            ORDER BY se.recid
        )                                                                          AS entry_seq
    FROM entries se
)

SELECT
    -- AccountNumber : t24_stmt_entry.account_number (direct)
    TRIM(CAST(d.account_number AS VARCHAR))                                        AS account_number,

    -- AcctName : ACCOUNT.TITLE.1 + SHORT.TITLE. NOTE (2026-06-23): fully NULL — the ACCOUNT join
    -- is a referential/vintage gap: 0 of 24,720 distinct t24_stmt_entry.account_number keys match
    -- t24_account.recid (disjoint account populations: stmt has USD-internal + 6/12-digit numbers
    -- absent from the 121,500-row account dim). No alternate account-title column exists in
    -- t24_stmt_entry. Stays NULL until a matching-vintage ACCOUNT snapshot is ingested. (acc.* all NULL.)
    TRIM(CONCAT_WS(' ',
        NULLIF(TRIM(CAST(acc.account_title_1[0] AS VARCHAR)), ''),
        NULLIF(TRIM(CAST(acc.short_title[0] AS VARCHAR)), '')
    ))                                                                             AS acct_name,

    -- OpenDate : ACCOUNT.OPENING.DATE (D2E->D4E in canonical; bronze is real DATE)
    CAST(acc.opening_date AS DATE)                                                 AS open_date,

    -- CustomerNo : t24_account.customer (L ACCOUNT,CUSTOMER)
    TRIM(CAST(acc.customer AS VARCHAR))                                            AS customer_no,

    -- CustomerName : title + name_1 + name_2 + short_name (canonical cust_name build)
    TRIM(CONCAT_WS(' ',
        NULLIF(TRIM(CAST(cus.title         AS VARCHAR)), ''),
        NULLIF(TRIM(CAST(cus.name_1[0]     AS VARCHAR)), ''),
        NULLIF(TRIM(CAST(cus.name_2[0]     AS VARCHAR)), ''),
        NULLIF(TRIM(CAST(cus.short_name[0] AS VARCHAR)), '')
    ))                                                                             AS cust_name,

    -- Occupation : t24_customer.occupation[0] (LIST)
    TRIM(CAST(cus.occupation[0] AS VARCHAR))                                       AS occupation,

    -- TIN : t24_customer.legal_id[0] (canonical legal_id / TIN)
    TRIM(CAST(cus.legal_id[0] AS VARCHAR))                                         AS tin_id,

    -- LegalType : EB.LOOKUP 'CUS.LEGAL.DOC.NAME*<code>' description[0];
    --             fallback -> GIC.ID description[0] when EB.LOOKUP yields NULL.
    COALESCE(
        NULLIF(TRIM(CAST(luk.lu_description AS VARCHAR)), ''),
        TRIM(CAST(gic.gic_description AS VARCHAR))
    )                                                                              AS legal_type,

    -- TxnCode : t24_stmt_entry.transaction_code (direct)
    TRIM(CAST(d.transaction_code AS VARCHAR))                                      AS txn_code,

    -- TxnDesc : t24_transaction.stmt_narr (TRANSACTION narrative for the txn code)
    TRIM(CAST(trn.stmt_narr AS VARCHAR))                                           AS txn_desc,

    -- TransRef : t24_stmt_entry.trans_reference (piece before ';' per canonical)
    TRIM(CAST(d.trans_reference AS VARCHAR))                                       AS trans_ref,

    -- ValueDate : t24_stmt_entry.value_date (canonical value date; real DATE)
    CAST(d.value_date AS DATE)                                                     AS value_date,

    -- Amount : AMOUNT.LCY from entry (absolute value; sign via dr/cr split below)
    ABS(CAST(d.amount_lcy AS DECIMAL(18,2)))                                       AS amount,

    -- DrAmount : debit portion (canonical DR.AMOUNT; amount when sign='-', else 0)
    d.dr_amount                                                                    AS dr_amount,

    -- CrAmount : credit portion (canonical CR.AMOUNT; amount when sign!='-', else 0)
    d.cr_amount                                                                    AS cr_amount,

    -- Narrative : entry narrative, with teller override for TT entries with blank narrative
    --   Canonical: IF system_id='TT' AND narrative IS blank -> use teller narrative_1 (or narrative_2)
    CASE WHEN TRIM(CAST(d.system_id AS VARCHAR)) = 'TT'
              AND (d.narrative[0] IS NULL OR TRIM(CAST(d.narrative[0] AS VARCHAR)) = '')
         THEN COALESCE(
                  NULLIF(TRIM(CAST(tel.narrative_1[0] AS VARCHAR)), ''),
                  TRIM(CAST(tel.narrative_2[0] AS VARCHAR))
              )
         ELSE TRIM(CAST(d.narrative[0] AS VARCHAR))
    END                                                                            AS narrative,

    -- WorkingBalance : t24_account.working_balance (static snapshot; AC.CashFlow not available)
    CAST(acc.working_balance AS DECIMAL(18,2))                                     AS working_balance,

    -- AvgBalance : AC.AVG.BAL routine (ACCT.ACTIVITY day-balances); sourced from
    -- t24_acct_activity (FBNK_ACCT_ACTIVITY) via acct_activity_dedup (latest month, deduped).
    -- balance[0] = first day-balance in the latest month's MV array; best-available approximation.
    -- Full AC.AVG.BAL formula (day-weighted, currency-rounded) is procedural; cannot be replicated in SQL.
    -- logic.md §Part1/step5h: "EbReadAcctActivityRecord(ACCID='<acct>-<YYYYMM>') walks day-balances."
    CAST(act.balance[0] AS DECIMAL(18,2))                                          AS avg_balance,

    -- CompanyCode : t24_stmt_entry.company_code (entry's own company; ACCOUNT join is broken
    -- vintage gap, so sourced from the entry rather than acc.co_code which is NULL).
    TRIM(CAST(d.company_code AS VARCHAR))                                          AS company_code,

    -- EntryCnt : count of today's entries for this account (canonical ENTRY.CNT)
    d.entry_cnt                                                                    AS entry_cnt,

    -- AcctTotal : sum of absolute amounts across today's entries (canonical Y.TOTAL.AMT)
    d.acct_total                                                                   AS acct_total,

    -- ── COVERAGE COLUMNS for rpt_9k_transaction ──────────────────────────────────
    -- These expose the same fields that the three legacy stg models exposed so that
    -- rpt_9k_transaction (and any future queries of this silver table) can join
    -- directly here without separate account/customer/txn_entry staging models.

    -- ── From stg_9k_transaction__txn_entry coverage ──
    -- transaction_id : STMT.ENTRY @ID (recid); key used in rpt surrogate
    TRIM(CAST(d.recid AS VARCHAR))                                                 AS transaction_id,

    -- application_id : transaction_code alias (same field already in txn_code)
    TRIM(CAST(d.transaction_code AS VARCHAR))                                      AS application_id,

    -- branch_code : t24_stmt_entry.company_code (entry's own company; same as company_code)
    TRIM(CAST(d.company_code AS VARCHAR))                                          AS branch_code,

    -- branch_name : canonical COMPANY.mnemonic via t24_company lookup keyed on the entry's
    -- company_code (replaces the prior hardcoded co_code->name CASE fed by the broken ACCOUNT
    -- join). Fallback to the raw company_code when the COMPANY record is absent.
    COALESCE(
        NULLIF(TRIM(CAST(com.mnemonic AS VARCHAR)), ''),
        TRIM(CAST(d.company_code AS VARCHAR))
    )                                                                              AS branch_name,

    -- account_id : account_number alias (same field already in account_number)
    TRIM(CAST(d.account_number AS VARCHAR))                                        AS account_id,

    -- account_customer_id : acc.customer (customer no. linked to the account)
    TRIM(CAST(acc.customer AS VARCHAR))                                            AS account_customer_id,

    -- operation_date : STMT.ENTRY booking_date (canonical operation/booking date)
    CAST(d.booking_date AS DATE)                                                   AS operation_date,

    -- transaction_currency : STMT.ENTRY.currency (ISO currency code of the entry)
    TRIM(CAST(d.currency AS VARCHAR))                                              AS transaction_currency,

    -- amount_ref_currency : STMT.ENTRY.amount_fcy (foreign/reference-currency amount)
    CAST(d.amount_fcy AS DECIMAL(18,2))                                            AS amount_ref_currency,

    -- debit_credit_indicator : derived from AMOUNT.LCY sign (canonical DR/CR split)
    CASE WHEN LEFT(TRIM(CAST(d.amount_lcy AS VARCHAR)), 1) = '-'
         THEN 'Debit' ELSE 'Credit'
    END                                                                            AS debit_credit_indicator,

    -- back_office_name : no bronze column for approving officer -> NULL (documented gap)
    CAST(NULL AS VARCHAR)                                                          AS back_office_name,

    -- transaction_approval_timestamp : booking_date cast to DATE (no timestamp on bronze)
    CAST(d.booking_date AS DATE)                                                   AS transaction_approval_timestamp,

    -- dir0 : compact YYYYMMDD partition key derived from booking_date
    REPLACE(CAST(CAST(d.booking_date AS DATE) AS VARCHAR), '-', '')                AS dir0,

    -- ── From stg_9k_transaction__accounts coverage ──
    -- account_title_1 : ACCOUNT.TITLE.1[0] (first element; LIST type)
    TRIM(CAST(acc.account_title_1[0] AS VARCHAR))                                  AS account_title_1,

    -- account_title_2 : ACCOUNT.TITLE.2[0] — t24_account.account_title_2 IS a LIST in bronze
    TRIM(CAST(acc.account_title_2[0] AS VARCHAR))                                  AS account_title_2,

    -- short_title : ACCOUNT.SHORT.TITLE[0] (LIST type)
    TRIM(CAST(acc.short_title[0] AS VARCHAR))                                      AS short_title,

    -- account_category : ACCOUNT.CATEGORY (product category code)
    TRIM(CAST(acc.category AS VARCHAR))                                            AS account_category,

    -- account_currency : ACCOUNT.CURRENCY confirmed present in hive.bronze.t24_account AT SNAPSHOT '{{ var("snap_account") }}'
    TRIM(CAST(acc.currency AS VARCHAR))                                            AS account_currency,

    -- record_timestamp : no static record-level timestamp on t24_account -> current_timestamp
    current_timestamp()                                                            AS record_timestamp,

    -- ── From stg_9k_transaction__customers coverage ──
    -- customer_id : t24_customer.recid (CUSTOMER @ID)
    TRIM(CAST(cus.recid AS VARCHAR))                                               AS customer_id,

    -- customer_name_1 : t24_customer.name_1[0] (LIST type; first name field)
    TRIM(CAST(cus.name_1[0] AS VARCHAR))                                           AS customer_name_1,

    -- customer_name_2 : t24_customer.name_2[0] (LIST type; second name field)
    TRIM(CAST(cus.name_2[0] AS VARCHAR))                                           AS customer_name_2,

    -- customer_full_name : concatenated name (name_1 + name_2 when available)
    TRIM(CONCAT_WS(' ',
        NULLIF(TRIM(CAST(cus.name_1[0] AS VARCHAR)), ''),
        NULLIF(TRIM(CAST(cus.name_2[0] AS VARCHAR)), '')
    ))                                                                             AS customer_full_name,

    -- customer_short_name : t24_customer.short_name[0] (LIST type)
    TRIM(CAST(cus.short_name[0] AS VARCHAR))                                       AS customer_short_name,

    -- legal_id : t24_customer.legal_id[0] alias (same data as tin_id; LIST type)
    TRIM(CAST(cus.legal_id[0] AS VARCHAR))                                         AS legal_id,

    -- legal_doc_type : t24_customer.legal_doc_name[0] (LIST; doc-type code raw)
    TRIM(CAST(cus.legal_doc_name[0] AS VARCHAR))                                   AS legal_doc_type,

    -- legal_doc_expiry_date : no legal_exp_date in documented bronze scope -> NULL (gap)
    CAST(NULL AS DATE)                                                             AS legal_doc_expiry_date,

    -- gender : t24_customer.gender (confirmed present in hive.bronze.t24_customer AT SNAPSHOT '{{ var("snap_customer") }}')
    TRIM(CAST(cus.gender AS VARCHAR))                                              AS gender,

    -- birth_or_incorp_date : t24_customer.date_of_birth (real DATE in bronze)
    CAST(cus.date_of_birth AS DATE)                                                AS birth_or_incorp_date,

    -- customer_type : no customer_type column in documented t24_customer scope -> NULL (gap)
    -- (acct_entry_no and com_consol_to removed: non-canonical sources t24_acct_ent_today
    --  and t24_company_consol dropped per phase-2 silver-source rebuild)
    CAST(NULL AS VARCHAR)                                                          AS customer_type,

    -- business_date: COB date stamp (T-STAMP; uniform across all 4 silver models)
    CAST(date '{{ var("business_date") }}' AS DATE)                AS business_date

FROM detail d

-- ACCOUNT enrichment: t24_account is 1 row/recid, no dedup CTE required.
LEFT JOIN hive.bronze.t24_account AT SNAPSHOT '{{ var("snap_account") }}'        acc
    ON TRIM(CAST(d.account_number AS VARCHAR)) = TRIM(CAST(acc.recid AS VARCHAR))

-- CUSTOMER enrichment (versioned dim; deduped to latest via curr_no DESC above).
LEFT JOIN customer_dedup cus
    ON TRIM(CAST(acc.customer AS VARCHAR)) = TRIM(CAST(cus.recid AS VARCHAR))

-- TRANSACTION param: transaction_code -> txn narrative/description (versioned, deduped).
LEFT JOIN transaction_dedup trn
    ON TRIM(CAST(d.transaction_code AS VARCHAR)) = TRIM(CAST(trn.recid AS VARCHAR))

-- TELLER enrichment: only TT-originated entries; teller @ID = piece before '\' in
-- trans_reference (which is itself the piece before ';' per canonical).
-- Condition guard (system_id = 'TT') on the JOIN so non-TT entries never match.
LEFT JOIN hive.bronze.t24_teller AT SNAPSHOT '{{ var("snap_teller") }}'         tel
    ON  TRIM(CAST(d.system_id AS VARCHAR)) = 'TT'
    AND TRIM(SPLIT_PART(SPLIT_PART(TRIM(CAST(d.trans_reference AS VARCHAR)), ';', 1), '\', 1))
        = TRIM(CAST(tel.recid AS VARCHAR))

-- EB.LOOKUP param: legal doc type code -> description[0].
-- Key pattern: 'CUS.LEGAL.DOC.NAME*<legal_doc_name[0]>' (canonical EB.LOOKUP read).
-- Dedup: bronze t24_eb_lookup carries multiple versioned rows per recid (curr_no); keep latest so
-- this lookup stays 1:1 and does NOT fan out the entry grain (reconciled 2026-06-22: 25 dup recids, ×3).
LEFT JOIN (
    SELECT recid, lu_description FROM (
        SELECT recid, description[0] AS lu_description,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR)) ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_eb_lookup WHERE recid LIKE 'CUS.LEGAL.DOC.NAME%' AND business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
) luk
    ON TRIM(CAST(luk.recid AS VARCHAR))
       = CONCAT('CUS.LEGAL.DOC.NAME*', TRIM(CAST(cus.legal_doc_name[0] AS VARCHAR)))

-- GIC.ID param: fallback legal doc type description when EB.LOOKUP yields NULL.
-- Dedup: t24_gic_id also versioned per recid (curr_no); keep latest (reconciled 2026-06-22: 15 dup recids, ×3).
LEFT JOIN (
    SELECT recid, gic_description FROM (
        SELECT recid, description[0] AS gic_description,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR)) ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_gic_id WHERE business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
) gic
    ON TRIM(CAST(gic.recid AS VARCHAR)) = TRIM(CAST(cus.legal_doc_name[0] AS VARCHAR))

-- COMPANY branch lookup: entry company_code -> mnemonic (versioned dim, deduped curr_no DESC).
LEFT JOIN company_dedup com
    ON TRIM(CAST(d.company_code AS VARCHAR)) = TRIM(CAST(com.recid AS VARCHAR))

-- ACCT.ACTIVITY (FBNK_ACCT_ACTIVITY): AC.AVG.BAL source for avg_balance.
-- Join key: acct_activity_dedup.account_no = d.account_number (account prefix extracted from recid).
-- logic.md §Part1/step5h: "EbReadAcctActivityRecord(ACCID='<acct>-<YYYYMM>') ... day-balances."
LEFT JOIN acct_activity_dedup act
    ON TRIM(act.account_no) = TRIM(CAST(d.account_number AS VARCHAR))

-- Canonical account-level gate (unconditional per canonical logic):
--   SUM(ABS(amount_lcy)) over all today-entries for the account >= 9000 (Y.TOTAL.AMT threshold)
--   AND last entry's currency = 'USD' (final Y.CCY gate — canonical uses last-entry CCY).
-- This keeps the ENTIRE entry block for qualifying accounts (not just entries >= 9000 individually).
WHERE d.acct_total >= 9000
  AND d.last_ccy   = 'USD'
-- ORDER BY removed: ORDER-BY columns (entry_seq) leak into the CTAS rowtype on dbt-dremio
-- (rowtype/field-name mismatch); a materialized table is unordered anyway.
