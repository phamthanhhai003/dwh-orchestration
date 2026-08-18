{{ config(
    materialized='table',
    database='hive',
    schema='silver'
) }}

-- T24 object: EM.AC.GIC.CLOSE  (driver class EM_EN_CLOSED_ACCOUNTS_cl)
-- SILVER-zone t24-built source table: one row per CLOSED account.
-- Canonical logic: T24_REPORT_DEVELOPMENT_SOT.md §10 "EM.AC.GIC.CLOSE".
--
-- Grain: one row per closed-account @ID. ACCOUNT.CLOSED (t24_account_closed)
-- supplies the driver id list; ACCOUNT$HIS (t24_account_his) supplies the closed
-- account's last image (EB.READ.HISTORY.REC) for the history-only columns.
--
-- 2026-06-22: ACCOUNT$HIS (hive.bronze.t24_account_his AT SNAPSHOT '{{ var("snap_account_his") }}') wired in.
--   Previously absent from bronze, so acct_title/category/ccy/credit_int/closing_bal/
--   arrangement_marker were CAST(NULL). Now sourced from t24_account_his, keyed by
--   account id = LPAD(account_closed.recid, 14, '0') = SPLIT_PART(his.recid, ';', 1),
--   deduped to the latest version (curr_no DESC). 7,332 of 101,000 closed accounts
--   have a $HIS image -> populated; the rest stay NULL (no history image — honest gap).
--   Field map (ACCOUNT$HIS): fld3 ACCOUNT.TITLE.1/.2 -> account_title_1[0]/account_title_2[0]
--   (ARRAY), fld2 CATEGORY -> category, fld8 CURRENCY -> currency, fld35 CREDIT.INT ->
--   acct_credit_int[0] (ARRAY), fld38 CLOSING.BAL -> working_balance, fld181 arrangement
--   marker -> arrangement_id (empty = classic AC, non-empty = AA).
-- POSTING.RESTRICT resolves the CLOSE.REASON description via LINK.
--
-- tables.md authoritative set: ACCOUNT.CLOSED + ACCOUNT$HIS + POSTING.RESTRICT.
-- CUSTOMER is NOT in this object's tables.md -> removed (was a foreign enrichment).
--
-- Audit 2026-06-19:
--   Fix 1: posting_restrict_dedup dedup ORDER BY corrected from recid DESC to
--           curr_no DESC (recurring bug found in 7/9 sibling models).
--   Fix 2: customer_dedup CTE and all customer name/enrichment columns removed
--           (CUSTOMER is a FOREIGN table not listed in this object's tables.md).
--   Fix 3: schema promoted bronze -> silver.
--
-- Bronze conventions (per stg__loan_sector_detail_t24.sql):
--   * LIST/multivalue bronze columns -> element access col[0] (NOT "col/0").
--   * Versioned dim tables (t24_posting_restrict) deduped to latest row per @ID
--     via ROW_NUMBER() PARTITION BY recid ORDER BY curr_no DESC.
--   * numeric id columns -> CAST(CAST(x AS BIGINT) AS VARCHAR) before LPAD.
--   * bronze date columns are real DATE -> CAST(x AS DATE); TO_CHAR for strings.
--   * close-date filter is UNCONDITIONAL (canonical: base variant LE TODAY, no var-gate).

WITH posting_restrict_dedup AS (
    -- POSTING.RESTRICT param table: latest row per @ID for the CLOSE.REASON LINK.
    -- Fix 2026-06-19: dedup ORDER BY curr_no DESC (was erroneously recid DESC).
    SELECT recid, description FROM (
        SELECT
            recid,
            description,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(CAST(recid AS VARCHAR))
                ORDER BY curr_no DESC
            ) AS rn
        FROM hive.bronze.t24_posting_restrict
    WHERE business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
),
-- ACCOUNT$HIS image: latest version per account id (recid = '<14-digit id>;<version>').
-- ARRAY columns (account_title_1/_2, acct_credit_int) -> element [0]. Latest = curr_no DESC.
-- ACCOUNT.CLOSED driver: dedup to 1 row per @ID. Bronze t24_account_closed carries
-- 1,000 duplicate recids (2,000 rows, near-empty twins — only 16 of the dup rows have a
-- populated branch/customer). The table has no curr_no; rank prefers the richest row
-- (account_branch populated first), then latest business_date. This removes the grain
-- fanout that previously produced 101,000 silver rows for 100,000 distinct closed accounts.
account_closed_dedup AS (
    SELECT recid, customer_id, account_branch, closure_reason, acct_close_date FROM (
        SELECT
            recid, customer_id, account_branch, closure_reason, acct_close_date,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(CAST(recid AS VARCHAR))
                ORDER BY CASE WHEN NULLIF(TRIM(CAST(account_branch AS VARCHAR)), '') IS NOT NULL THEN 0 ELSE 1 END,
                         business_date DESC
            ) AS rn
        FROM hive.bronze.t24_account_closed AT SNAPSHOT '{{ var("snap_account_closed") }}'
    ) t WHERE rn = 1
),
account_his_dedup AS (
    SELECT acct_id, acct_title, category_raw, currency, credit_int_raw, closing_bal, arrangement_marker FROM (
        SELECT
            SPLIT_PART(CAST(recid AS VARCHAR), ';', 1)                                 AS acct_id,
            TRIM(CONCAT_WS(' ',
                 NULLIF(TRIM(CAST(account_title_1[0] AS VARCHAR)), ''),
                 NULLIF(TRIM(CAST(account_title_2[0] AS VARCHAR)), '')))               AS acct_title,
            TRIM(CAST(category AS VARCHAR))                                            AS category_raw,
            NULLIF(TRIM(CAST(currency AS VARCHAR)), '')                                AS currency,
            TRIM(CAST(acct_credit_int[0] AS VARCHAR))                                  AS credit_int_raw,
            CAST(working_balance AS DECIMAL(18,2))                                     AS closing_bal,
            NULLIF(TRIM(CAST(arrangement_id AS VARCHAR)), '')                          AS arrangement_marker,
            ROW_NUMBER() OVER (
                PARTITION BY SPLIT_PART(CAST(recid AS VARCHAR), ';', 1)
                ORDER BY CAST(curr_no AS INTEGER) DESC
            ) AS rn
        FROM hive.bronze.t24_account_his AT SNAPSHOT '{{ var("snap_account_his") }}'
    ) t WHERE rn = 1
)

SELECT
    -- ACCT.ID (NOFILE output pos 1 = closed-account @ID): closed account id.
    LPAD(TRIM(CAST(CAST(ac.recid AS BIGINT) AS VARCHAR)), 14, '0')                     AS acct_id,
    -- CUSTOMER (t24_account_closed.customer_id): customer id.
    TRIM(CAST(ac.customer_id AS VARCHAR))                                              AS customer,
    -- BRANCH (t24_account_closed.account_branch): account branch code.
    -- Sourced from ACCOUNT.CLOSED driver table (not ACCOUNT$HIS).
    -- Gold counterpart: branch_name. Not in Part 2 SQL but is an ACCOUNT.CLOSED attribute.
    TRIM(CAST(ac.account_branch AS VARCHAR))                                           AS branch_code,
    -- ACCT.TITLE (ACCOUNT$HIS fld3): account_title_1[0] + account_title_2[0] concatenated.
    his.acct_title                                                                    AS acct_title,
    -- CATEGORY (ACCOUNT$HIS fld2): numeric category code (guarded cast; non-numeric/empty -> NULL).
    CASE WHEN REGEXP_LIKE(his.category_raw, '^[0-9]+$')
         THEN CAST(his.category_raw AS INTEGER) END                                   AS category,
    -- CCY (ACCOUNT$HIS fld8): account currency.
    his.currency                                                                      AS ccy,
    -- CREDIT.INT (ACCOUNT$HIS fld35): credit-interest condition value. Guarded numeric cast
    -- (loan accounts carry empty/non-numeric CREDIT.INT -> NULL).
    CASE WHEN REGEXP_LIKE(his.credit_int_raw, '^-?[0-9]+(\.[0-9]+)?$')
         THEN CAST(his.credit_int_raw AS DECIMAL(18,2)) END                           AS credit_int,
    -- CLOSING.BAL (ACCOUNT$HIS fld38): balance carried on the closed image (working_balance).
    his.closing_bal                                                                   AS closing_bal,
    -- CLOSE.DATE (t24_account_closed.acct_close_date, TYPE.8 = D): real DATE.
    CAST(ac.acct_close_date AS DATE)                                                   AS close_date,
    -- CLOSE.REASON (t24_account_closed.closure_reason = raw POSTING.RESTRICT code).
    TRIM(CAST(ac.closure_reason AS VARCHAR))                                           AS close_reason_code,
    -- CLOSE.REASON description: LINK L POSTING.RESTRICT,DESCRIPTION resolved at display layer.
    TRIM(CAST(pr.description[0] AS VARCHAR))                                           AS close_reason,
    -- ARRANGEMENT.MARKER (ACCOUNT$HIS fld181 AC/AA gate): empty = classic AC account,
    -- non-empty (an AA arrangement id) = AA account. Now sourced from ACCOUNT$HIS.
    his.arrangement_marker                                                            AS arrangement_marker,
    -- @ID (output buffer): the closed-account @ID (t24_account_closed recid).
    TRIM(CAST(ac.recid AS VARCHAR))                                                    AS id,
    CAST(date '{{ var("business_date") }}' AS DATE)                AS business_date
FROM account_closed_dedup ac
-- CLOSE.REASON description LINK (display-layer LEFT JOIN).
LEFT JOIN posting_restrict_dedup pr ON TRIM(CAST(ac.closure_reason AS VARCHAR)) = TRIM(CAST(pr.recid AS VARCHAR))
-- ACCOUNT$HIS image LINK: closed-account id padded to 14 = his account id (recid before ';').
LEFT JOIN account_his_dedup his ON his.acct_id = LPAD(TRIM(CAST(CAST(ac.recid AS BIGINT) AS VARCHAR)), 14, '0')
-- Canonical: base EM.AC.GIC.CLOSE variant applies ACCT.CLOSE.DATE <= TODAY (CURRENT_DATE) unconditionally.
WHERE CAST(ac.acct_close_date AS DATE) <= date '{{ var("business_date") }}'
