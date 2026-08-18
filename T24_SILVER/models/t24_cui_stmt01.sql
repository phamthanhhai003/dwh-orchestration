{{ config(
    materialized='table',
    database='hive',
    schema='silver'
) }}

-- Division: credit | Object: CUI.STMT01 (t24-built source)
-- Reads real hive.bronze.t24_* per Part-1 §7 CUI.STMT01 (T24_REPORT_DEVELOPMENT_SOT.md).
-- Per-account dated statement-entry listing: one row per STMT.ENTRY (or netted
-- STMT.ENTRY.DETAIL) line. Implements the E.STMT.ENQ.BY.CONCAT / E.READ.STMT.ENTRY
-- NOFILE driver logic in SQL.
--
-- Bronze conventions (see stg__loan_sector_detail_t24.sql):
--   * Array/multivalue bronze columns are LIST type -> element access col[0] (NOT "col/0").
--   * Dimension dedup: t24_customer carries multiple versioned rows per recid -> keep
--     latest version per recid (ROW_NUMBER ... ORDER BY recid DESC) so joins stay 1:1.
--   * numeric id columns -> CAST(CAST(x AS BIGINT) AS VARCHAR) before LPAD (avoid .0).
--   * Bronze date columns are real DATE -> CAST(x AS DATE). Use TO_CHAR for string dates.
--   * Active-row / period filters gated behind var('require_current', true) so dev
--     coverage can drop them.
--
-- Grain: one row per resolved statement-entry record (the §7 concat-element-2 id).
--   Plain entries come straight from t24_stmt_entry; netted entries (id contains '!')
--   are exploded to their detail rows via t24_stmt_entry_detail_xref ->
--   t24_stmt_entry_detail.
--
-- §7 mechanics applied:
--   * Brought-forward balance source (t24_account_statement) is absent in bronze ->
--     bfwd_balance / period-base CAST(NULL) (STMT.PRINTED walk-back path not available).
--   * Amount (Field.32 'CCY CCY M'): FCY when present, else LCY ->
--     COALESCE(NULLIF(amount_fcy,''), amount_lcy).
--   * Currency fallback to t24_account.currency when STMT.ENTRY currency missing.
--   * display_name (PRTNM Field.16): IF CUS NE NULL -> customer_short_name else account_short_title.
--   * period_end_total (TOTAL Field.69): C ENT.TOT + B.AMT.BF -- running total per account
--     break (B.ACC) plus brought-forward balance (NULL base -> running entry total).

WITH customer_dedup AS (
    SELECT recid, short_name FROM (
        SELECT recid, short_name[0] AS short_name,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_customer
    ) t WHERE rn = 1
),

-- §7 brought-forward balance: ACCOUNT.STATEMENT (wired 2026-06-19; keyed by recid = account number).
-- curr_no is boolean in this table (current-record flag); DESC puts TRUE before FALSE.
acct_stmt_dedup AS (
    SELECT recid, fqu1_last_balance FROM (
        SELECT recid, fqu1_last_balance,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_account_statement
    ) t WHERE rn = 1
),

-- §7 STMT.PRINTED walk-back marker (wired 2026-06-20; keyed by recid = account number).
-- stmt_entry_no = the last statement-entry number printed for the account (opening-balance
-- walk-back anchor). 0 rows in dev -> NULL, populates in a prod snapshot.
stmt_printed_dedup AS (
    SELECT recid, stmt_entry_no FROM (
        SELECT TRIM(CAST(recid AS VARCHAR)) AS recid, TRIM(CAST(stmt_entry_no AS VARCHAR)) AS stmt_entry_no,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY TRIM(CAST(recid AS VARCHAR)) DESC) AS rn
        FROM hive.bronze.t24_stmt_printed
    ) t WHERE rn = 1
),

-- §7 AC.STMT.PARAMETER (wired 2026-06-21; keyed by literal "SYSTEM" — static cross-join,
-- same pattern as archive_stmt in t24_stmt_ent_book_cui).
-- AcStmtParameter_CacheRead("SYSTEM",...) -> AcStpFwdMvmtReqd (logic.md §Part1/step3).
-- curr_no is CHARACTER VARYING -> ORDER BY curr_no DESC picks latest authorised record.
-- 0 or 1 row in dev; cross-join safe after dedup to 1 row.
ac_stmt_param_dedup AS (
    SELECT fwd_mvmt_reqd FROM (
        SELECT TRIM(CAST(fwd_mvmt_reqd AS VARCHAR)) AS fwd_mvmt_reqd,
               ROW_NUMBER() OVER (ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_ac_stmt_parameter
        WHERE TRIM(CAST(recid AS VARCHAR)) = 'SYSTEM'
    ) t WHERE rn = 1
),

-- §7 COMPANY.SMS.GROUP (wired 2026-06-21; keyed by account.co_code = sms_group.recid).
-- getST_CompanyCreation().tableCompanySmsGroup(companyId) — resolves company group for
-- cross-book access (logic.md §Part1/step4). description is ARRAY -> [0].
-- curr_no is CHARACTER VARYING -> ORDER BY curr_no DESC.
company_sms_group_dedup AS (
    SELECT recid, description FROM (
        SELECT recid, description[0] AS description,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_company_sms_group
    ) t WHERE rn = 1
),

-- §7 USER (wired 2026-06-21; keyed by stmt_entry.inputter[0] = user.recid).
-- getEB_SystemTables().getRUser() -> User_UseOthBookAccess (logic.md §Part1/step4).
-- inputter is ARRAY on t24_stmt_entry -> join on inputter[0] (first element = creating user).
-- curr_no is CHARACTER VARYING -> ORDER BY curr_no DESC.
user_dedup AS (
    SELECT recid, user_name FROM (
        SELECT recid, TRIM(CAST(user_name AS VARCHAR)) AS user_name,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_user
    ) t WHERE rn = 1
),

-- §7 TRANSACTION: L TRANSACTION,NARRATIVE display-link (canonical: narrative/stmt_narr).
-- Versioned dim -> dedup to latest version per recid via curr_no DESC (2 rows per recid
-- confirmed in sibling models; same pattern as t24_stmt_ent_book_cui.sql).
transaction_dedup AS (
    SELECT recid, stmt_narr FROM (
        SELECT recid, stmt_narr,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_transaction
    ) t WHERE rn = 1
),

-- Plain (non-netted) STMT.ENTRY rows: id does NOT contain '!'.
plain_entries AS (
    SELECT
        TRIM(CAST(se.account_number AS VARCHAR))                                          AS account_number,
        TRIM(CAST(se.recid AS VARCHAR))                                                   AS stmt_entry_id,
        CAST(se.value_date AS DATE)                                                       AS value_date,
        CAST(se.booking_date AS DATE)                                                     AS booking_date,
        TRIM(CAST(se.transaction_code AS VARCHAR))                                        AS transaction_code,
        se.narrative                                                                      AS narrative_arr,
        TRIM(CAST(se.trans_reference AS VARCHAR))                                         AS trans_reference,
        COALESCE(NULLIF(CAST(se.amount_fcy AS VARCHAR), ''),
                 CAST(se.amount_lcy AS VARCHAR))                                          AS amount_raw,
        TRIM(CAST(se.currency AS VARCHAR))                                                AS currency,
        TRIM(CAST(se.system_id AS VARCHAR))                                               AS system_id,
        -- §7 USER join key: inputter[0] = the creating user id (logic.md §Part1/step4).
        TRIM(CAST(se.inputter[0] AS VARCHAR))                                             AS inputter_user
    FROM hive.bronze.t24_stmt_entry se
    WHERE CAST(se.recid AS VARCHAR) NOT LIKE '%!%'
),

-- Netted STMT.ENTRY rows (id contains '!') exploded via XREF -> STMT.ENTRY.DETAIL.
netted_entries AS (
    SELECT
        TRIM(CAST(se.account_number AS VARCHAR))                                          AS account_number,
        TRIM(CAST(sed.recid AS VARCHAR))                                                  AS stmt_entry_id,
        CAST(sed.value_date AS DATE)                                                      AS value_date,
        CAST(sed.booking_date AS DATE)                                                    AS booking_date,
        TRIM(CAST(sed.transaction_code AS VARCHAR))                                       AS transaction_code,
        sed.narrative                                                                     AS narrative_arr,
        TRIM(CAST(sed.trans_reference AS VARCHAR))                                        AS trans_reference,
        COALESCE(NULLIF(CAST(sed.amount_fcy AS VARCHAR), ''),
                 CAST(sed.amount_lcy AS VARCHAR))                                         AS amount_raw,
        TRIM(CAST(sed.currency AS VARCHAR))                                               AS currency,
        TRIM(CAST(sed.system_id AS VARCHAR))                                              AS system_id,
        -- §7 USER join key: inputter[0] from the detail record (logic.md §Part1/step4).
        TRIM(CAST(sed.inputter[0] AS VARCHAR))                                            AS inputter_user
    FROM hive.bronze.t24_stmt_entry se
    JOIN hive.bronze.t24_stmt_entry_detail_xref xr
        ON TRIM(CAST(se.recid AS VARCHAR)) = TRIM(CAST(xr.recid AS VARCHAR))
    JOIN hive.bronze.t24_stmt_entry_detail sed
        ON TRIM(CAST(xr.detail_id AS VARCHAR)) = TRIM(CAST(sed.recid AS VARCHAR))
    WHERE CAST(se.recid AS VARCHAR) LIKE '%!%'
),

all_entries AS (
    SELECT * FROM plain_entries
    UNION ALL
    SELECT * FROM netted_entries
)

SELECT
    e.account_number                                                                     AS account_number,
    e.stmt_entry_id                                                                      AS stmt_entry_id,
    CAST(ast.fqu1_last_balance AS DECIMAL(18,2))                                         AS bfwd_balance,
    e.value_date                                                                         AS value_date,
    e.booking_date                                                                       AS booking_date,
    e.transaction_code                                                                   AS transaction_code,
    TRIM(CAST(trn.stmt_narr AS VARCHAR))                                                 AS description,
    e.narrative_arr                                                                      AS narrative,
    e.trans_reference                                                                    AS trans_reference,
    CAST(e.amount_raw AS DECIMAL(18,2))                                                  AS amount,
    COALESCE(NULLIF(e.currency, ''), TRIM(CAST(acc.currency AS VARCHAR)))                AS currency,
    TRIM(CAST(acc.short_title[0] AS VARCHAR))                                            AS account_short_title,
    TRIM(CAST(acc.customer AS VARCHAR))                                                  AS customer_id,
    TRIM(CAST(cus.short_name AS VARCHAR))                                                AS customer_short_name,
    CASE
        WHEN acc.customer IS NOT NULL AND TRIM(CAST(acc.customer AS VARCHAR)) <> ''
            THEN TRIM(CAST(cus.short_name AS VARCHAR))
        ELSE TRIM(CAST(acc.short_title[0] AS VARCHAR))
    END                                                                                  AS display_name,
    -- §7 EB.SYSTEM.ID drilldown application (logic.md §10: next appl from EB.SYSTEM.ID).
    -- join key = system_id (ebs.recid); 0-match in dev -> NULL in dev environment.
    TRIM(CAST(ebs.application AS VARCHAR))                                               AS drilldown_application,
    -- §7 running_balance: bfwd_balance + cumulative entry amount per account (logic.md Part-2 lines 104-110).
    -- Canonical: ob.bfwd_balance + SUM(signed_amount) OVER (PARTITION BY account ORDER BY booking_date, entry_id ROWS UNBOUNDED).
    CAST(ast.fqu1_last_balance AS DECIMAL(18,2))
    + SUM(CAST(e.amount_raw AS DECIMAL(18,2))) OVER (
        PARTITION BY e.account_number
        ORDER BY e.booking_date, e.stmt_entry_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                                                    AS running_balance,
    -- §7 period_end_total: bfwd_balance + sum(all entry amounts) per account (logic.md Part-2 line 112).
    -- Canonical: ob.bfwd_balance + SUM(signed_amount) OVER (PARTITION BY account_no) — partition-wide, not running.
    CAST(ast.fqu1_last_balance AS DECIMAL(18,2))
    + SUM(CAST(e.amount_raw AS DECIMAL(18,2))) OVER (
        PARTITION BY e.account_number
    )                                                                                    AS period_end_total,
    -- §7 STMT.PRINTED last printed entry no per account (wired 2026-06-20; 0-row dev -> NULL)
    sp.stmt_entry_no                                                                     AS last_printed_entry_no,
    -- §7 AC.STMT.PARAMETER fwd_mvmt_reqd (wired 2026-06-21; static "SYSTEM" record;
    -- AcStpFwdMvmtReqd controls forward-movement handling; logic.md §Part1/step3).
    asp.fwd_mvmt_reqd                                                                    AS stmt_fwd_mvmt_reqd,
    -- §7 COMPANY.SMS.GROUP description (wired 2026-06-21; keyed by account co_code;
    -- company group for cross-book access; logic.md §Part1/step4). 0-match in dev -> NULL.
    csg.description                                                                      AS sms_group_description,
    -- §7 USER user_name (wired 2026-06-21; keyed by stmt_entry.inputter[0];
    -- User_UseOthBookAccess entitlement check; logic.md §Part1/step4). 0-match in dev -> NULL.
    usr.user_name                                                                        AS inputter_user_name
FROM all_entries e
LEFT JOIN hive.bronze.t24_account           acc ON e.account_number = TRIM(CAST(acc.recid AS VARCHAR))
LEFT JOIN customer_dedup                    cus ON TRIM(CAST(acc.customer AS VARCHAR)) = TRIM(CAST(cus.recid AS VARCHAR))
LEFT JOIN transaction_dedup                 trn ON e.transaction_code = TRIM(CAST(trn.recid AS VARCHAR))
LEFT JOIN acct_stmt_dedup                   ast ON e.account_number = TRIM(CAST(ast.recid AS VARCHAR))
-- §7 EB.SYSTEM.ID (wired 2026-06-19): system_id key 0-match in dev -> drilldown_application NULL.
LEFT JOIN hive.bronze.t24_eb_system_id      ebs ON TRIM(CAST(e.system_id AS VARCHAR)) = TRIM(CAST(ebs.recid AS VARCHAR))
LEFT JOIN stmt_printed_dedup                sp  ON e.account_number = TRIM(CAST(sp.recid AS VARCHAR))
-- §7 AC.STMT.PARAMETER (wired 2026-06-21): static "SYSTEM" cross-join; 0 or 1 row after dedup.
LEFT JOIN ac_stmt_param_dedup               asp ON 1=1
-- §7 COMPANY.SMS.GROUP (wired 2026-06-21): keyed by account co_code = sms_group recid.
LEFT JOIN company_sms_group_dedup           csg ON TRIM(CAST(acc.co_code AS VARCHAR)) = TRIM(CAST(csg.recid AS VARCHAR))
-- §7 USER (wired 2026-06-21): keyed by stmt_entry.inputter[0] = user.recid (logic.md §Part1/step4).
LEFT JOIN user_dedup                        usr ON TRIM(CAST(e.inputter_user AS VARCHAR)) = TRIM(CAST(usr.recid AS VARCHAR))
