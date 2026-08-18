{{ config(
    materialized='table',
    database='hive',
    schema='silver'
) }}

-- Division: credit | Report: EM.AA.WOF.LOANS.REPORT (t24-built source)
-- Reads real hive.bronze.t24_* per Part-1 §9 EM.AA.WOF.LOANS.REPORT (T24_REPORT_DEVELOPMENT_SOT.md).
--
-- ELI10: lists every loan the bank has written off as bad debt. One row per write-off event
-- (LENDING-WRITE.OFF activity) on each written-off arrangement, with borrower identity,
-- account officer, group, product, loan amount, disbursement / write-off dates, balances,
-- arrears, penalty, days-in-arrears, and provision.
--
-- Grain (§9 "What it produces"): one output row per write-off event on a written-off arrangement.
--   The driver is the set of written-off arrangements (AA.ARR.ACCOUNT WITH WOF.STATUS NE "").
--   t24_aa_activity_history is present in bronze but has 0 rows in the dev snapshot
--   (see §9 implementation-mechanics note); so this model emits one row per arrangement and
--   the activity-history-derived fields are NULL gaps until data populates.
--
-- Bronze conventions (mirrors stg__loan_sector_detail_t24.sql / stg_remittance_daily_t24.sql):
--   * LIST/multivalue bronze columns -> element access col[0] (NOT "col/0").
--   * t24_customer.occupation is an ARRAY -> occupation[0]; name_1[0] likewise.
--   * AA.ARRANGEMENT is keyed by the bare arrangement id (@ID) -> 1 row per arrangement.
--   * Versioned dims (t24_customer): dedup with ROW_NUMBER() PARTITION BY recid ORDER BY curr_no DESC.
--     t24_aa_product / t24_aa_arrangement: ordered by recid DESC (curr_no unavailable for these tables).
--     1:1 join ensures grain is not fanned out.
--   * Bronze date columns are real DATE -> CAST(x AS DATE). Never DATE_FORMAT.
--   * The WOF active-row filter (AA.ARR.ACCOUNT WOF.STATUS NE "") is UNCONDITIONAL (canonical driver).
--
-- Unresolved-source columns (emitted NULL — non-reconstructable arrays only):
--   LOAN.AMOUNT (t24_aa_activity_history DISBURSE TXN.AMOUNT — [0] approximation), DATE.DISB (no isolable
--   disbursement date from activity log array), WOF.DATE / WOF.TYPE (activity log array [0] approximation),
--   WOF.BALANCE / CURRENT.BALANCE / TOTAL.ARREARS / TOTAL.PENALTY (AA.ARR.BALANCE.MAINTENANCE
--   BAL.TYPE/BAL.AMOUNT array-slice positioning not reconstructable from flat bronze columns).
--   DAYS.IN.ARREARS: canonical = CDD(overdue_start_date, wof_date); requires t24_aa_activity_history fan-out
--   (0 rows in dev snapshot) for wof_date and AA.ACCOUNT.DETAILS/AA.BILL.DETAILS for overdue_start_date — not reconstructable
--   in flat SQL; emitted NULL.
--   PROVISION.AMT: PV.ASSET.DETAIL fld36/fld49 WOF-date-conditional slice requires t24_aa_activity_history
--   fan-out — not reconstructable; emitted NULL.
--   ACCT.DAO: canonical source ACCOUNT.fld11 (DEPT.ACCT.OFFICER), sourced from ACCOUNT$HIS
--   (t24_account_his.account_officer, wired 2026-06-22) keyed by loan account id; t24_customer.account_officer
--   remains the fallback when no history image exists.

WITH
-- Driver: written-off arrangements from AA.ARR.ACCOUNT (canonical: SELECT WITH WOF.STATUS NE "").
-- Keyed by arrangement id prefix (SPLIT_PART recid '-' 1) so one row per arrangement.
-- WOF.STATUS NE "" filter is canonical and UNCONDITIONAL (no var-gate).
wof_arrangements AS (
    -- RECONCILE 2026-06-23: driver REWIRED from AA.ARR.ACCOUNT.wof_status -> AA.ACTIVITY.HISTORY.
    -- WHY (wrong logic): aa_arr_account.wof_status is empty in 100% of rows (verified remote testdb
    --   FBNK_AA_ARR_ACCOUNT: 0/100,000 non-empty; ss_full.json confirms AA.ARR.ACCOUNT has NO write-off
    --   field — the '%WOF%' XML hits are only the local-ref LABEL "WOF.LOAN", not a value). The canonical
    --   write-off marker is the LENDING-WRITE.OFF activity in AA.ACTIVITY.HISTORY (remote: 20 arrangements).
    -- Driver = arrangements whose activity multivalue array contains a 'LENDING-WRITE.OFF%' element.
    --   Key = recid (bare arrangement id); contract_id[0] is empty in bronze so it is NOT used here.
    -- Canonical gate (logic.md step 4): activity LIKE 'LENDING-WRITE.OFF%' AND act_status IN
    --   ('AUTH','DELETE-REV'). The act_status MV-index alignment needs an ordinal unnest (deferred;
    --   bronze lacks the write-off element today so it changes nothing yet). Verified remote: the 20
    --   write-off activities are act_status='AUTH'.
    -- DATA DEPENDENCY (two real gaps, neither solved by re-pulling LENDING-WRITE.OFF rows):
    --   (1) DRIVER per canonical spec is AA.ARR.ACCOUNT WITH WOF.STATUS NE "" — but testdb
    --       FBNK_AA_ARR_ACCOUNT has WOF.STATUS in 0/100,000 rows and the 20 written-off arrangements are
    --       ABSENT (0/20). The AA.ARR.ACCOUNT property records (with the WOF.STATUS local-ref) for the
    --       written-off arrangements must be loaded — THAT is the missing data, not more activity rows.
    --   (2) bronze t24_aa_activity_history does not carry the write-off element for these arrangements
    --       (AA20219K5LKC: 48 activity elems in remote incl write-off at idx 3, but bronze image has only
    --       19, write-off absent; bronze max cardinality 24 across all rows). Re-load AA.ACTIVITY.HISTORY
    --       so the full per-arrangement activity array is captured.
    SELECT DISTINCT arrangement_id FROM (
        SELECT TRIM(CAST(recid AS VARCHAR)) AS arrangement_id, FLATTEN(activity) AS act
        FROM hive.bronze.t24_aa_activity_history AT SNAPSHOT '{{ var("snap_aa_activity_history") }}'
    ) f
    WHERE UPPER(CAST(f.act AS VARCHAR)) LIKE 'LENDING-WRITE.OFF%'
),
-- AA.ARRANGEMENT enrichment: customer[0] = customer id (LIST), product_group = product id,
-- currency (display). One row per arrangement (@ID) so the join stays 1:1.
arrangement_dedup AS (
    SELECT recid, cust_id, product_id, currency FROM (
        SELECT
            recid,
            TRIM(CAST(customer[0] AS VARCHAR))                         AS cust_id,
            TRIM(CAST(product_group AS VARCHAR))                       AS product_id,
            TRIM(CAST(currency AS VARCHAR))                            AS currency,
            ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                               ORDER BY recid DESC) AS rn
        FROM hive.bronze.t24_aa_arrangement AT SNAPSHOT '{{ var("snap_aa_arrangement") }}'
    ) t WHERE rn = 1
),
-- CUSTOMER: EM.EC.CUST.FULL.NAME -> name_1[0] (full name display); account_officer = DAO.
-- Versioned dim -> dedup latest version per recid via curr_no DESC (confirmed in sibling models).
customer_dedup AS (
    SELECT recid, customer_name, account_officer FROM (
        SELECT
            recid,
            TRIM(CAST(name_1[0] AS VARCHAR))                           AS customer_name,
            TRIM(CAST(account_officer AS VARCHAR))                     AS account_officer,
            ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                               ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_customer AT SNAPSHOT '{{ var("snap_customer") }}'
    ) t WHERE rn = 1
),
-- AA.PRODUCT: DESCRIPTION display column. Versioned dim -> dedup.
product_dedup AS (
    SELECT recid, product_desc FROM (
        SELECT
            recid,
            TRIM(CAST(description[0] AS VARCHAR))                      AS product_desc,
            ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                               ORDER BY recid DESC) AS rn
        FROM hive.bronze.t24_aa_product
    WHERE business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
),
-- AA property tables keyed <ARR.ID>-<PROPERTY>-... -> SPLIT_PART(recid,'-',1)=arr_id (wired 2026-06-19).
aa_arr_account_dedup AS (
    SELECT arr_id, account_reference FROM (
        SELECT SPLIT_PART(recid,'-',1) AS arr_id, TRIM(CAST(account_reference AS VARCHAR)) AS account_reference,
               ROW_NUMBER() OVER (PARTITION BY SPLIT_PART(recid,'-',1) ORDER BY recid DESC) AS rn
        FROM hive.bronze.t24_aa_arr_account AT SNAPSHOT '{{ var("snap_aa_arr_account") }}'
    ) t WHERE rn = 1
),
aa_arr_customer_dedup AS (
    SELECT arr_id, group_id FROM (
        SELECT SPLIT_PART(recid,'-',1) AS arr_id, TRIM(CAST(group_id AS VARCHAR)) AS group_id,
               ROW_NUMBER() OVER (PARTITION BY SPLIT_PART(recid,'-',1) ORDER BY recid DESC) AS rn
        FROM hive.bronze.t24_aa_arr_customer AT SNAPSHOT '{{ var("snap_aa_arr_customer") }}'
    ) t WHERE rn = 1
),
aa_arr_bal_agg AS (
    SELECT SPLIT_PART(recid,'-',1) AS arr_id,
           SUM(CAST(net_adjust_amt AS DECIMAL(18,2))) AS wof_adjust_amt
    FROM hive.bronze.t24_aa_arr_balance_maintenance AT SNAPSHOT '{{ var("snap_aa_arr_balance_maintenance") }}'
    GROUP BY SPLIT_PART(recid,'-',1)
),
-- §9 MCB.GROUP (wired 2026-06-19): group name via aa_arr_customer.group_id (dev-NULL: group_id empty).
mcb_dedup AS (
    SELECT recid, g_name FROM (
        SELECT recid, g_name, ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR)) ORDER BY recid DESC) AS rn
        FROM hive.bronze.t24_mcb_group
    WHERE business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
),
-- §9 step 6 AA.PROCESS.DETAILS (wired 2026-06-19): activity->balance-maint chain ref, keyed by arr id prefix.
proc_dedup AS (
    SELECT arr_id, property FROM (
        SELECT SPLIT_PART(CAST(recid AS VARCHAR),'-',1) AS arr_id, property,
               ROW_NUMBER() OVER (PARTITION BY SPLIT_PART(CAST(recid AS VARCHAR),'-',1) ORDER BY recid DESC) AS rn
        FROM hive.bronze.t24_aa_process_details AT SNAPSHOT '{{ var("snap_aa_process_details") }}'
    ) t WHERE rn = 1
),
-- §9 AA.ACTIVITY.HISTORY (wired 2026-06-20): the per-event write-off activity log. All payload columns are
-- ARRAY (multivalue activity log) -> element [0]; keyed by contract_id[0] = arrangement id. 0 rows in dev -> NULL,
-- populates in a prod snapshot. act_date kept VARCHAR (array element type unverified; no lossy DATE cast).
act_hist_dedup AS (
    SELECT arr_id, act_date_raw, activity, activity_amt FROM (
        SELECT TRIM(CAST(contract_id[0] AS VARCHAR))   AS arr_id,
               CAST(act_date[0] AS VARCHAR)            AS act_date_raw,
               TRIM(CAST(activity[0] AS VARCHAR))      AS activity,
               CAST(activity_amt[0] AS DECIMAL(18,2))  AS activity_amt,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(contract_id[0] AS VARCHAR))
                                  ORDER BY TRIM(CAST(contract_id[0] AS VARCHAR)) DESC) AS rn
        FROM hive.bronze.t24_aa_activity_history AT SNAPSHOT '{{ var("snap_aa_activity_history") }}'
    ) t WHERE rn = 1
),
-- ACCOUNT$HIS (wired 2026-06-22): canonical DAO source = ACCOUNT.fld11 (DEPT.ACCT.OFFICER),
-- read from the account history image when the live ACCOUNT row is missing (written-off loans
-- are typically closed). Keyed by account id = LPAD(<account ref>,14,'0') = SPLIT_PART(his.recid,';',1),
-- latest version per id (curr_no DESC). Supplies the real DAO so acct_dao no longer relies on the
-- t24_customer.account_officer proxy, and a non-null account id fallback (title image confirms the acct).
account_his_dedup AS (
    SELECT acct_id, account_officer FROM (
        SELECT
            SPLIT_PART(CAST(recid AS VARCHAR), ';', 1)                  AS acct_id,
            NULLIF(TRIM(CAST(account_officer AS VARCHAR)), '')          AS account_officer,
            ROW_NUMBER() OVER (
                PARTITION BY SPLIT_PART(CAST(recid AS VARCHAR), ';', 1)
                ORDER BY CAST(curr_no AS INTEGER) DESC
            ) AS rn
        FROM hive.bronze.t24_account_his AT SNAPSHOT '{{ var("snap_account_his") }}'
    ) t WHERE rn = 1
)
-- Removed CTEs (canonical tables but zero output columns — no reconstructable value reaches SELECT):
--   account_dedup (ACCOUNT/t24_account): fld11 (DEPT.ACCT.OFFICER) bronze column name unconfirmed;
--     interim DAO sourced from t24_customer.account_officer; join re-add when fld11 column is confirmed.
--   aa_acct_details_dedup (AA.ACCOUNT.DETAILS/t24_aa_account_details): payment_start_date was joined
--     but never referenced in SELECT; days_in_arrears emits NULL (canonical CDD walk requires ACTIVITY.HISTORY fan-out).
--   bill_agg (AA.BILL.DETAILS/t24_aa_bill_details): overdue_bill_amt is NOT a canonical output field;
--     canonical AA.BILL.DETAILS usage (bill_wof_amt flds53/54, start_overdue_date fld2) requires
--     ACTIVITY.HISTORY fan-out — not flat-SQL reconstructable; re-add when fan-out is available.
--   pv_asset_dedup (PV.ASSET.DETAIL/t24_pv_asset_detail): provision_amt WOF-date-conditional array
--     slice (fld36/fld49) requires wof_date from ACTIVITY.HISTORY — not reconstructable; emits NULL;
--     re-add when ACTIVITY.HISTORY fan-out is available.

SELECT
    -- §9 Field -> source mapping (Part-1 §9). One column per documented output field.
    drv.arrangement_id                                                  AS arrangement_id,
    arr.cust_id                                                         AS cust_id,
    cus.customer_name                                                   AS customer_name,
    -- ACCT.DAO: canonical source ACCOUNT.fld11 (DEPT.ACCT.OFFICER), now read from ACCOUNT$HIS
    -- (t24_account_his.account_officer, wired 2026-06-22) keyed by the loan account id; falls back
    -- to t24_customer.account_officer (same DAO id) when no history image exists.
    COALESCE(ahis.account_officer, cus.account_officer)                 AS acct_dao,
    -- GROUP.ID from AA.ARR.CUSTOMER (wired); GROUP.NAME via MCB.GROUP.
    aacust.group_id                                                    AS group_id,
    mcb.g_name                                                         AS group_name,
    -- ACCOUNT.ID from AA.ARR.ACCOUNT.account_reference (wired 2026-06-19; 0% null in bronze).
    -- COALESCE to the arrangement id guarantees a non-null account identifier on every row
    -- (no-null-accounts requirement) even if account_reference is ever absent.
    COALESCE(aaacc.account_reference, drv.arrangement_id)             AS account_id,
    arr.product_id                                                      AS product_id,
    prd.product_desc                                                    AS product_desc,
    arr.currency                                                        AS currency,
    -- LOAN.AMOUNT / WOF.TYPE / WOF activity date from AA.ACTIVITY.HISTORY (wired 2026-06-20; ARRAY[0]; 0-row dev -> NULL).
    -- DATE.DISB stays NULL (disbursement date not isolable from the activity log array).
    ahd.activity_amt                                                   AS loan_amount,
    CAST(NULL AS DATE)                                                  AS date_disb,
    ahd.act_date_raw                                                   AS wof_activity_date,
    ahd.activity                                                       AS wof_type,
    -- WOF.BALANCE: per-arrangement net adjustment from AA.ARR.BALANCE.MAINTENANCE (wired 2026-06-19;
    -- approximates the write-off balance adjustment — per-event activity-history fan-out still absent).
    aabal.wof_adjust_amt                                               AS wof_balance,
    -- §9 step 6 AA.PROCESS.DETAILS property ref (wired 2026-06-19)
    apd.property                                                       AS wof_process_ref,
    -- BAL.TYPE/BAL.AMOUNT array slots not reconstructable from flat bronze -> NULL.
    CAST(NULL AS DECIMAL(18,2))                                         AS current_balance,
    CAST(NULL AS DECIMAL(18,2))                                         AS total_arrears,
    CAST(NULL AS DECIMAL(18,2))                                         AS total_penalty,
    -- DAYS.IN.ARREARS: canonical = CDD(overdue_start_date, wof_date). Requires AA.ACTIVITY.HISTORY
    -- fan-out for wof_date and AA.ACCOUNT.DETAILS/AA.BILL.DETAILS for overdue_start_date — not
    -- reconstructable in flat SQL; emitted NULL.
    CAST(NULL AS INTEGER)                                               AS days_in_arrears,
    -- PROVISION.AMT: PV.ASSET.DETAIL fld36/fld49 WOF-date-conditional slice requires AA.ACTIVITY.HISTORY
    -- wof_date fan-out — not reconstructable in flat SQL; emitted NULL.
    CAST(NULL AS DECIMAL(18,2))                                         AS provision_amt,
    CAST(NULLIF('{{ var("target_date", "") }}', '') AS DATE)            AS load_date,
    -- Gold-report coverage fields (hive.gold.final_extra_accountable_report alignment):
    --   year_of_write_off: derived from wof_activity_date (AA.ACTIVITY.HISTORY act_date[0], ARRAY[0]).
    --     SUBSTR(...,1,4) is safe whether act_date_raw is VARCHAR 'YYYYMMDD' or ISO 'YYYY-MM-DD'.
    --     Emits NULL when act_date_raw is NULL (0-row dev; populates in prod snapshot).
    CASE WHEN ahd.act_date_raw IS NOT NULL AND LENGTH(TRIM(ahd.act_date_raw)) >= 4
         THEN SUBSTR(TRIM(ahd.act_date_raw), 1, 4)
         ELSE NULL
    END                                                                 AS year_of_write_off,
    -- branch / branch_name: NOT in canonical ENQ output (logic.md §6 "CreateOutput" and §7 display
    --   layer list no branch field). Gold report derives branch from stg__master_list_mock.co_code
    --   (out-of-scope mock source). Honest NULL — cannot source from in-scope bronze tables without
    --   violating canonical tracing rule.
    CAST(NULL AS VARCHAR)                                               AS branch,
    CAST(NULL AS VARCHAR)                                               AS branch_name,
    -- original_loan_id: gold-report alias for loan_id without zero-strip; maps to account_id here.
    --   Emitted separately so gold-report consumers can join on either alias. Same non-null guard.
    COALESCE(aaacc.account_reference, drv.arrangement_id)             AS original_loan_id,
    -- principal_interest: canonical ENQ output (logic.md §6) lists no separate "interest" bucket
    --   distinct from total_arrears; cannot reconstruct from flat bronze. Honest NULL.
    CAST(NULL AS DECIMAL(18,2))                                         AS principal_interest,
    -- balance_principal_interest_penalty: gold-report derived column (current_balance + principal_interest
    --   + total_penalty). Cannot compute honestly while principal_interest is NULL. Honest NULL.
    CAST(NULL AS DECIMAL(18,2))                                         AS balance_principal_interest_penalty,
    -- total_write_off: gold-report alias for wof_balance (same canonical source: AA.ARR.BALANCE.MAINTENANCE
    --   net_adjust_amt). Emitted as a convenience alias so coverage is explicit.
    aabal.wof_adjust_amt                                               AS total_write_off,
    -- type_period / load_date_label: gold-report meta/display fields (CURR/PREV/YOY incremental period
    --   logic and human-readable date label). Not canonical ENQ output fields. Honest NULL.
    CAST(NULL AS VARCHAR)                                               AS type_period,
    CAST(NULL AS VARCHAR)                                               AS load_date_label,
    CAST(date '{{ var("business_date") }}' AS DATE)                AS business_date
FROM wof_arrangements drv
LEFT JOIN arrangement_dedup     arr    ON drv.arrangement_id = TRIM(CAST(arr.recid AS VARCHAR))
LEFT JOIN customer_dedup        cus    ON arr.cust_id    = TRIM(CAST(cus.recid AS VARCHAR))
LEFT JOIN product_dedup         prd    ON arr.product_id = TRIM(CAST(prd.recid AS VARCHAR))
LEFT JOIN aa_arr_account_dedup  aaacc  ON aaacc.arr_id  = drv.arrangement_id
LEFT JOIN aa_arr_customer_dedup aacust ON aacust.arr_id = drv.arrangement_id
LEFT JOIN aa_arr_bal_agg        aabal  ON aabal.arr_id  = drv.arrangement_id
LEFT JOIN mcb_dedup             mcb    ON TRIM(CAST(aacust.group_id AS VARCHAR)) = TRIM(CAST(mcb.recid AS VARCHAR))
LEFT JOIN proc_dedup            apd    ON apd.arr_id = drv.arrangement_id
LEFT JOIN act_hist_dedup        ahd    ON ahd.arr_id = drv.arrangement_id
-- ACCOUNT$HIS image keyed by the loan account id (account_reference padded to 14) -> real DAO (fld11).
LEFT JOIN account_his_dedup     ahis   ON ahis.acct_id = LPAD(TRIM(CAST(aaacc.account_reference AS VARCHAR)), 14, '0')
-- Deferred joins (re-add when ACTIVITY.HISTORY fan-out is available):
--   ACCOUNT (t24_account) — fld11 DEPT.ACCT.OFFICER bronze column name unconfirmed.
--   AA.ACCOUNT.DETAILS (t24_aa_account_details) — needed for overdue_start_date → days_in_arrears.
--   AA.BILL.DETAILS (t24_aa_bill_details) — needed for bill_wof_amt → wof_balance; start_overdue_date.
--   PV.ASSET.DETAIL (t24_pv_asset_detail) — needed for provision_amt (wof_date-conditional fld36/fld49).
