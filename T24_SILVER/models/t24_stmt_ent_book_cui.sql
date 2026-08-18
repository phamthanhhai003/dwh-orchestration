{{ config(
    materialized='table',
    database='hive',
    schema='silver'
) }}

-- Division: credit | Object: BNCTL.STMT.ENT.BOOK.CUI (t24-built source, SILVER zone)
-- Reads real hive.bronze.t24_* per Part-1 §5 BNCTL.STMT.ENT.BOOK.CUI
-- (T24_sourcing_doc/T24_REPORT_DEVELOPMENT_SOT.md).
--
-- What it produces (§5): a dated account-statement-entry listing over STMT.ENTRY for an
-- account, sorted BOOKING.DATE then DATE.TIME, with per-row debit/credit split, running
-- balance, and per-account break totals.
--
-- Grain: one row per STMT.ENTRY record (driver = t24_stmt_entry, the §5 record key).
--
-- Join chain (§5 "Join / LINK chain"):
--   t24_stmt_entry (driver) -> t24_account       on account_number = account.recid
--   t24_account             -> t24_category       on account.category = category.recid
--   t24_account             -> t24_customer       on account.customer = customer.recid
--   t24_account             -> t24_company        on account.co_code  = company.recid
--   t24_stmt_entry          -> t24_transaction    on transaction_code = transaction.recid
--   t24_stmt_entry          -> t24_eb_system_id   on system_id = eb_system_id.recid (base_appl)
--   t24_customer.suburb_town    -> t24_bnctl_cus_dist.recid   (address district name)
--   t24_customer.city_municipal -> t24_bnctl_cus_subdist.recid (address sub-district name)
--   t24_customer.province_state -> t24_bnctl_cus_vill.recid   (address village/ward name)
--   t24_archive  keyed 'STATEMENT'                             (purge date for opening-bal window)
--   t24_ac_account_sweep_hist on trans_ref+SW/PO (ACCP/ACSW entries only; 0-row dev -> NULL)
--   t24_acct_stmt_print per account (0-row dev -> NULL)
--   t24_ac_sub_account  per account (0-row dev -> NULL)
--
-- Bronze conventions (C1 pilot):
--   * Array / multivalue bronze columns are LIST type -> element access col[0] (NOT "col/0").
--   * Versioned dimension tables (t24_category, t24_customer) carry >1 row per recid ->
--     dedup latest via ROW_NUMBER() OVER (PARTITION BY recid ORDER BY curr_no DESC).
--   * Bronze date columns are real DATE -> CAST(x AS DATE). Never DATE_FORMAT.
--   * numeric id columns -> CAST(CAST(x AS BIGINT) AS VARCHAR) before LPAD (avoid .0).
--   * Active-row filter gated behind var('require_current') so dev coverage can drop it.
--
-- Audit fixes 2026-06-19 (vs bronze version):
--   (f) category_dedup: was ORDER BY recid DESC (wrong) -> fixed to curr_no DESC;
--       short_name[0] extracted inside CTE (not outside) to avoid double-array access.
--   (f) customer_dedup: was ORDER BY recid DESC (wrong) -> fixed to curr_no DESC;
--       added suburb_town, city_municipal, province_state for BNCTL.STMT.ADDRESS lookups.
--   (b) eb_system_id join key: was acc.recid (wrong) -> fixed to p.system_id
--       (STMT.ENTRY.SYSTEM_ID is the key for E.READ.EB.SYSTEM.ID per canonical).
--   (b) bnctl_cus_dist join key: was acc.co_code (wrong) -> fixed to cus.suburb_town
--       (canonical: CUSTOMER.SUBURB.TOWN local-ref -> BNCTL.CUS.DIST.recid).
--   (b) bnctl_cus_subdist join key: was acc.co_code (wrong) -> fixed to cus.city_municipal
--       (canonical: CUSTOMER.CITY.MUNICIPAL local-ref -> BNCTL.CUS.SUBDIST.recid).
--   (b) bnctl_cus_vill join key: was acc.co_code (wrong) -> fixed to cus.province_state
--       (canonical: CUSTOMER.PROVINCE.STATE local-ref -> BNCTL.CUS.VILL.recid).
--   (b) archive join key: was acc.recid (wrong) -> fixed to static 'STATEMENT' lookup
--       (canonical: ARCHIVE keyed by string "STATEMENT" for purge boundary).
--   (b) ac_account_sweep_hist join key: was acc.recid (wrong) -> fixed to trans_reference
--       with ACCP/ACSW system_id filter (E.TRN.REF.CONV reads "<ref>-SW"/"-PO").
--   (a) output field names: subdist_ref (recid) -> subdist_name (district_id);
--       vill_ref (recid) -> vill_name (subdistrict_id) — canonical lookup returns name.
--   (f) bnctl_cus_dist/subdist/vill/company/transaction/eb_system_id: all are versioned dims
--       (2x rows per recid) -> added dedup CTEs. curr_no DESC for dist/subdist/company/txn;
--       recid fallback for vill and eb_system_id (curr_no is NULL in those tables).
--   (f) archive_stmt: ARCHIVE has 2 STATEMENT rows in dev -> added ROW_NUMBER dedup to 1 row
--       before cross-join (prevents 2x fan-out).
--       Net grain fix: 1,179 stmt_entry rows (post-filter) = 1,179 silver rows (1:1 confirmed).
--   schema: bronze -> silver.
--
-- Derived-via-routine gaps (no resolvable bronze column — see gaps[]):
--   * opening_balance  -> @ BNCTL.E.CALC.OPEN.BALANCE : AC balance engine API (EbGetAcctBalance),
--                         no single bronze column. NULL.
--   * locked_amount    -> @ BNCTL.E.STMT.LOCK.AMOUNT : AcGetBalWithLock AC API. NULL.
--   * base_appl        -> @ E.READ.EB.SYSTEM.ID : joined from t24_eb_system_id.application.
--   * running_balance / balance_at_period_end : accumulator part computed as
--     SUM(amount) OVER (...). The opening_balance term is NULL (AC API), so these carry
--     the cumulative entry-balance only (documented deviation).
--   * acct_stmt_print / ac_sub_account: 0-row in dev -> NULL (documented gap).
--   * ac_account_sweep_hist: 0-row in dev -> NULL (documented gap).

WITH category_dedup AS (
    -- t24_category: versioned dim -> dedup to latest version per recid using curr_no DESC.
    -- short_name is a LIST -> extract [0] here inside CTE.
    SELECT recid, short_name FROM (
        SELECT recid, short_name[0] AS short_name,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_category
    ) t WHERE rn = 1
),
customer_dedup AS (
    -- t24_customer: versioned dim -> dedup to latest version per recid using curr_no DESC.
    -- short_name and name_1 are LIST -> extract [0] here inside CTE (same pattern as category_dedup).
    -- Expose local-ref fields for BNCTL.STMT.ADDRESS lookups and name fields for header.
    SELECT recid, short_name, name_1, street, town_country,
           suburb_town, city_municipal, province_state FROM (
        SELECT recid, short_name[0] AS short_name, name_1[0] AS name_1, street, town_country,
               suburb_town, city_municipal, province_state,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_customer
    ) t WHERE rn = 1
),
-- BNCTL.CUS.DIST: versioned dim -> dedup via curr_no DESC.
-- recid = SUBURB.TOWN local-ref code; dist_name is LIST -> [0].
bnctl_cus_dist_dedup AS (
    SELECT recid, dist_name FROM (
        SELECT recid, dist_name[0] AS dist_name,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_bnctl_cus_dist
    ) t WHERE rn = 1
),
-- BNCTL.CUS.SUBDIST: versioned dim -> dedup via curr_no DESC.
-- recid = CITY.MUNICIPAL local-ref code.
bnctl_cus_subdist_dedup AS (
    SELECT recid, district_id FROM (
        SELECT recid, district_id,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_bnctl_cus_subdist
    ) t WHERE rn = 1
),
-- BNCTL.CUS.VILL: versioned dim -> dedup by recid (curr_no is NULL in this table).
-- recid = PROVINCE.STATE local-ref code.
bnctl_cus_vill_dedup AS (
    SELECT recid, subdistrict_id FROM (
        SELECT recid, subdistrict_id,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY TRIM(CAST(recid AS VARCHAR)) DESC) AS rn
        FROM hive.bronze.t24_bnctl_cus_vill
    ) t WHERE rn = 1
),
-- t24_company: versioned dim -> dedup via curr_no DESC (2 rows per recid confirmed).
-- company_name is a LIST -> extract [0] here inside CTE.
company_dedup AS (
    SELECT recid, company_name, local_currency FROM (
        SELECT recid, company_name[0] AS company_name, local_currency,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_company
    ) t WHERE rn = 1
),
-- t24_transaction: versioned dim -> dedup via curr_no DESC (2 rows per recid confirmed).
transaction_dedup AS (
    SELECT recid, stmt_narr FROM (
        SELECT recid, stmt_narr,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_transaction
    ) t WHERE rn = 1
),
-- t24_eb_system_id: versioned dim -> dedup by recid (curr_no is NULL in this table; 2 rows per recid).
eb_system_id_dedup AS (
    SELECT recid, application FROM (
        SELECT recid, application,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY TRIM(CAST(recid AS VARCHAR)) DESC) AS rn
        FROM hive.bronze.t24_eb_system_id
    ) t WHERE rn = 1
),
-- §5 record key = one STMT.ENTRY row. SHOW.REVERSAL filter not applied (full-fidelity DWH
-- capture, documented deviation). Optional date-window filters omitted (header-only at silver).
entries AS (
    SELECT * FROM hive.bronze.t24_stmt_entry
),
-- Per-row amount selection (currency-equality rule, §5 Field.49). Computed first so the
-- DR/CR split and the running-balance accumulator can reuse it.
priced AS (
    SELECT
        se.*,
        cmp.local_currency                                                      AS comp_local_currency,
        CASE
            WHEN SUBSTRING(TRIM(CAST(cmp.local_currency AS VARCHAR)), 1, 3)
               = SUBSTRING(TRIM(CAST(se.currency AS VARCHAR)), 1, 3)
            THEN CAST(se.amount_lcy AS DECIMAL(18,2))
            ELSE CAST(se.amount_fcy AS DECIMAL(18,2))
        END                                                                     AS amount_sel
    FROM entries se
    LEFT JOIN hive.bronze.t24_account  acc
        ON TRIM(CAST(se.account_number AS VARCHAR)) = TRIM(CAST(acc.recid AS VARCHAR))
    LEFT JOIN company_dedup  cmp
        ON TRIM(CAST(acc.co_code AS VARCHAR)) = TRIM(CAST(cmp.recid AS VARCHAR))
),
-- §5 ARCHIVE keyed 'STATEMENT' -> purge boundary date for opening-balance window.
-- Single static lookup; dedup to 1 row (STATEMENT has 2 rows in dev) then LEFT JOIN ON 1=1.
archive_stmt AS (
    SELECT purge_date FROM (
        SELECT purge_date,
               ROW_NUMBER() OVER (ORDER BY purge_date DESC) AS rn
        FROM hive.bronze.t24_archive
        WHERE TRIM(CAST(recid AS VARCHAR)) = 'STATEMENT'
    ) t WHERE rn = 1
),
-- §5 ACCT.STMT.PRINT printed-statement balance per account (0-row dev -> NULL).
-- t24_acct_stmt_print has no curr_no -> dedup by recid (no better key available).
acct_stmt_print_dedup AS (
    SELECT recid, stmt_date_bal FROM (
        SELECT TRIM(CAST(recid AS VARCHAR)) AS recid,
               TRIM(CAST(stmt_date_bal AS VARCHAR)) AS stmt_date_bal,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY TRIM(CAST(recid AS VARCHAR)) DESC) AS rn
        FROM hive.bronze.t24_acct_stmt_print
    ) t WHERE rn = 1
),
-- §5 AC.SUB.ACCOUNT sub-account membership per master account (0-row dev -> NULL).
-- t24_ac_sub_account has no curr_no -> dedup by recid (no better key available).
ac_sub_account_dedup AS (
    SELECT recid, sub_account FROM (
        SELECT TRIM(CAST(recid AS VARCHAR)) AS recid,
               TRIM(CAST(sub_account AS VARCHAR)) AS sub_account,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY TRIM(CAST(recid AS VARCHAR)) DESC) AS rn
        FROM hive.bronze.t24_ac_sub_account
    ) t WHERE rn = 1
),

-- §5 MNEMONIC.COMPANY (wired 2026-06-21; PP path only; medium confidence).
-- E.READ.EB.SYSTEM.ID reads MNEMONIC.COMPANY keyed by PP company-ref extracted from
-- STMT.ENTRY.SYSTEM.ID when the system_id maps to PP (logic.md §Part1/step9).
-- Join key: p.system_id -> mnemonic_company.recid (closest bronze approximation;
-- 0-row match expected in dev — PP path inactive in dev snapshot).
-- No curr_no in this table -> dedup by recid DESC.
mnemonic_company_dedup AS (
    SELECT recid, company FROM (
        SELECT TRIM(CAST(recid AS VARCHAR)) AS recid,
               TRIM(CAST(company AS VARCHAR)) AS company,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY TRIM(CAST(recid AS VARCHAR)) DESC) AS rn
        FROM hive.bronze.t24_mnemonic_company
    ) t WHERE rn = 1
)

SELECT
    -- Header / identity
    TRIM(CAST(p.account_number AS VARCHAR))                                      AS account,
    -- original_acct / auto_pay_acct have no bronze column on t24_account (gaps)
    CAST(NULL AS VARCHAR)                                                        AS original_acct,
    CAST(NULL AS VARCHAR)                                                        AS auto_pay_acct,
    TRIM(CAST(cat.short_name AS VARCHAR))                                        AS category_name,
    -- IBAN = ALT.ACCT.ID second value-mark element (T24 V 2 -> Dremio 0-indexed [1])
    TRIM(CAST(acc.alt_acct_id[1] AS VARCHAR))                                    AS iban,
    TRIM(CAST(acc.customer AS VARCHAR))                                          AS customer_id,
    TRIM(CAST(cus.short_name AS VARCHAR))                                        AS customer_short_name,
    TRIM(CAST(cus.name_1 AS VARCHAR))                                            AS customer_name,
    TRIM(CAST(cmp.company_name AS VARCHAR))                                      AS company_name,
    TRIM(CAST(cmp.local_currency AS VARCHAR))                                    AS comp_ccy,
    -- Address: @ BNCTL.STMT.ADDRESS concatenates dist/subdist/vill resolved from customer
    -- local-ref codes (SUBURB.TOWN, CITY.MUNICIPAL, PROVINCE.STATE).
    -- dist_name is already scalar (extracted as [0] inside bnctl_cus_dist_dedup CTE).
    CONCAT_WS(', ',
        TRIM(CAST(bd.dist_name AS VARCHAR)),
        TRIM(CAST(bsd.district_id AS VARCHAR)),
        TRIM(CAST(bv.subdistrict_id AS VARCHAR))
    )                                                                           AS customer_address_district,
    -- Opening balance / locked amount: AC balance-engine API, no bronze column (gaps)
    CAST(NULL AS DECIMAL(18,2))                                                  AS opening_balance,
    CAST(NULL AS DECIMAL(18,2))                                                  AS locked_amount,

    -- Per-row statement entry
    CAST(p.booking_date AS DATE)                                                 AS post_date,
    TRIM(CAST(p.trans_reference AS VARCHAR))                                     AS trans_ref,
    TRIM(CAST(trn.stmt_narr AS VARCHAR))                                         AS transaction_narrative,
    p.narrative                                                                  AS narrative,
    CAST(p.value_date AS DATE)                                                   AS value_date,
    p.amount_sel                                                                 AS amount,
    CASE WHEN p.amount_sel < 0 THEN ABS(p.amount_sel) END                        AS dr_amt,
    CASE WHEN p.amount_sel > 0 THEN p.amount_sel END                             AS cr_amt,
    -- Running balance: accumulator part only (opening_balance term is NULL — AC API gap)
    SUM(p.amount_sel) OVER (
        PARTITION BY TRIM(CAST(p.account_number AS VARCHAR))
        ORDER BY CAST(p.booking_date AS DATE), TRIM(CAST(p.recid AS VARCHAR))
        ROWS UNBOUNDED PRECEDING
    )                                                                           AS running_balance,
    TRIM(CAST(p.their_reference AS VARCHAR))                                     AS their_reference,
    TRIM(CAST(p.system_id AS VARCHAR))                                           AS system_id,
    -- base_appl: E.READ.EB.SYSTEM.ID maps STMT.ENTRY.SYSTEM_ID -> SID_APPLICATION (base app for drill-down)
    TRIM(CAST(ebs.application AS VARCHAR))                                       AS base_appl,

    -- Per-account break totals (§5 Implementation mechanics — B.ACC break)
    SUM(CASE WHEN p.amount_sel < 0 THEN ABS(p.amount_sel) END) OVER (
        PARTITION BY TRIM(CAST(p.account_number AS VARCHAR)))                    AS total_dr_amt,
    SUM(CASE WHEN p.amount_sel > 0 THEN p.amount_sel END) OVER (
        PARTITION BY TRIM(CAST(p.account_number AS VARCHAR)))                    AS total_cr_amt,
    SUM(CASE WHEN p.amount_sel < 0 THEN 1 ELSE 0 END) OVER (
        PARTITION BY TRIM(CAST(p.account_number AS VARCHAR)))                    AS dr_txn_count,
    SUM(CASE WHEN p.amount_sel > 0 THEN 1 ELSE 0 END) OVER (
        PARTITION BY TRIM(CAST(p.account_number AS VARCHAR)))                    AS cr_txn_count,
    -- balance_at_period_end = SUM(all amounts for account) (+ opening_balance gap)
    SUM(p.amount_sel) OVER (
        PARTITION BY TRIM(CAST(p.account_number AS VARCHAR)))                    AS balance_at_period_end,

    -- §5 BNCTL.STMT.ADDRESS district lookups (keys: customer local-ref codes; deduped)
    -- dist_name is scalar (extracted [0] inside bnctl_cus_dist_dedup CTE)
    TRIM(CAST(bd.dist_name AS VARCHAR))                                          AS dist_name,
    TRIM(CAST(bsd.district_id AS VARCHAR))                                       AS subdist_name,
    TRIM(CAST(bv.subdistrict_id AS VARCHAR))                                     AS vill_name,
    -- §5 AC.ACCOUNT.SWEEP.HIST (ACCP/ACSW entries; 0-row dev -> NULL)
    TRIM(CAST(ash.recid AS VARCHAR))                                             AS sweep_ref,
    -- §5 ARCHIVE 'STATEMENT' purge boundary date
    CAST(arc.purge_date AS DATE)                                                 AS archive_purge_date,
    -- §5 ACCT.STMT.PRINT / AC.SUB.ACCOUNT (0-row dev -> NULL)
    CAST(asp.stmt_date_bal AS DECIMAL(18,2))                                     AS stmt_print_balance,
    TRIM(CAST(asa.sub_account AS VARCHAR))                                       AS sub_account_ref,
    -- §5 MNEMONIC.COMPANY company ref (wired 2026-06-21; PP path only, medium confidence;
    -- E.READ.EB.SYSTEM.ID reads MNEMONIC.COMPANY for PP system-ids; logic.md §Part1/step9).
    -- 0-row match in dev (PP path inactive in dev snapshot) -> NULL.
    mc.company                                                                   AS mnemonic_company_ref
    -- updated_at removed (strict pass 3): CURRENT_TIMESTAMP has no canonical logic.md backing
FROM priced p
LEFT JOIN hive.bronze.t24_account       acc
    ON TRIM(CAST(p.account_number AS VARCHAR)) = TRIM(CAST(acc.recid AS VARCHAR))
LEFT JOIN category_dedup cat
    ON CAST(CAST(acc.category AS BIGINT) AS VARCHAR) = TRIM(CAST(cat.recid AS VARCHAR))
LEFT JOIN customer_dedup cus
    ON TRIM(CAST(acc.customer AS VARCHAR)) = TRIM(CAST(cus.recid AS VARCHAR))
LEFT JOIN company_dedup                 cmp
    ON TRIM(CAST(acc.co_code AS VARCHAR)) = TRIM(CAST(cmp.recid AS VARCHAR))
LEFT JOIN transaction_dedup             trn
    ON TRIM(CAST(p.transaction_code AS VARCHAR)) = TRIM(CAST(trn.recid AS VARCHAR))
-- E.READ.EB.SYSTEM.ID: STMT.ENTRY.SYSTEM_ID -> EB.SYSTEM.ID.recid (base application for drill-down)
LEFT JOIN eb_system_id_dedup                ebs ON TRIM(CAST(p.system_id AS VARCHAR)) = TRIM(CAST(ebs.recid AS VARCHAR))
-- BNCTL.STMT.ADDRESS: customer local-ref codes -> deduped district / sub-district / village lookups
LEFT JOIN bnctl_cus_dist_dedup              bd  ON TRIM(CAST(cus.suburb_town AS VARCHAR)) = TRIM(CAST(bd.recid AS VARCHAR))
LEFT JOIN bnctl_cus_subdist_dedup           bsd ON TRIM(CAST(cus.city_municipal AS VARCHAR)) = TRIM(CAST(bsd.recid AS VARCHAR))
LEFT JOIN bnctl_cus_vill_dedup              bv  ON TRIM(CAST(cus.province_state AS VARCHAR)) = TRIM(CAST(bv.recid AS VARCHAR))
-- E.TRN.REF.CONV: AC.ACCOUNT.SWEEP.HIST keyed "<trans_ref>-SW" for ACCP/ACSW entries (0-row dev -> NULL)
LEFT JOIN hive.bronze.t24_ac_account_sweep_hist ash
    ON  TRIM(CAST(p.system_id AS VARCHAR)) IN ('ACCP', 'ACSW')
    AND TRIM(CAST(p.trans_reference AS VARCHAR)) || '-SW' = TRIM(CAST(ash.recid AS VARCHAR))
-- BNCTL.E.CALC.OPEN.BALANCE: ARCHIVE keyed 'STATEMENT' for purge boundary date (cross-join safe: 1 row)
LEFT JOIN archive_stmt arc ON 1=1
LEFT JOIN acct_stmt_print_dedup             asp ON TRIM(CAST(acc.recid AS VARCHAR)) = TRIM(CAST(asp.recid AS VARCHAR))
LEFT JOIN ac_sub_account_dedup              asa ON TRIM(CAST(acc.recid AS VARCHAR)) = TRIM(CAST(asa.recid AS VARCHAR))
-- §5 MNEMONIC.COMPANY (wired 2026-06-21): PP path; keyed by system_id -> mnemonic_company.recid
-- (logic.md §Part1/step9). 0-row in dev -> NULL.
LEFT JOIN mnemonic_company_dedup            mc  ON TRIM(CAST(p.system_id AS VARCHAR)) = TRIM(CAST(mc.recid AS VARCHAR))
-- SHOW.REVERSAL: canonical ENQ selection filter (EQ, user-supplied); bronze has no
-- show_reversal column and the proxy (system_id NOT LIKE '%REV%') is non-canonical
-- (system_id never encodes reversal marker in STMT.ENTRY). Full-fidelity DWH silver
-- captures all entries; reversal filtering belongs at the reporting/enquiry layer.
-- No WHERE filter applied (documented deviation; see comment at line 159).
