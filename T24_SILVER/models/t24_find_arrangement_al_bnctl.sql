{{ config(
    materialized='table',
    database='hive',
    schema='silver'
) }}

-- Division: credit | Object: EM.FIND.ARRANGEMENT.AL.BNCTL (t24-built source)
-- Reads real hive.bronze.t24_* per Part-1 §12 (T24_REPORT_DEVELOPMENT_SOT.md).
-- One row per active LENDING arrangement at BNCTL (BREAK CHANGE @ID → 1 row per arrangement).
--
-- Bronze conventions applied:
--   * LIST/multivalue bronze columns → element access col[0] (NOT "col/0").
--   * occupation on t24_customer is an ARRAY → occupation[0] (not used here; name_1 LIST → name_1[0]).
--   * AA property tables keyed by composite recid <ARR.ID>-<PROPERTY>-<DATE>.<seq>
--       → join via SPLIT_PART(recid,'-',1) = arrangement_id; aggregate to one row per arrangement.
--   * Versioned dims (t24_customer) deduped via ROW_NUMBER() keeping latest curr_no DESC.
--   * numeric id columns → CAST(CAST(x AS BIGINT) AS VARCHAR) before LPAD (avoid .0).
--   * Bronze date columns are real DATE → CAST(x AS DATE); never DATE_FORMAT.
--   * Active-row filter (fixed selections §12) is UNCONDITIONAL (canonical: ARR.STATUS NOT IN UNAUTH/CANCELLED/CLOSE/MATURED).
--   * Reserved-word alias 'at' replaced with 'atm' (AT is reserved in Dremio/Calcite).
--
-- Unresolved-source columns (emitted NULL — see §12 implementation mechanics & gaps[]):
--   TOTCOMMBAL, CURRACCBAL, DUE.TODAY, RESCH.IND,
--   ARR.STATUS.DISP-from-activity-history.
--   CATEGORY_CODE: t24_account.category not in tables.md (EM.FIND.ARRANGEMENT.AL.BNCTL scope) → honest NULL.
-- Wired (was NULL): PRODUCT.DESC via t24_aa_product (product_group join; 2026-06-19).
-- Wired (2026-06-20): COMMITMENT_AMT via t24_aa_arr_term_amount COMMITMENT records
--   (best-effort substitute for unresolvable TOTCOMMBAL; AA.ARR.TERM.AMOUNT in tables.md;
--    used by stg_credit_disbursement_t24 as total_disbursement proxy).
-- Wired (2026-06-21): t24_em_dl_application re-added — tables.md=high, logic.md §Part1/step9 backs
--   EM.EC.AA.GET.LOAN.TERM DlApplication_CacheRead; key=dl_appl_id from arr_customer_dedup; emits dl_app_date.
-- Removed (2026-06-22): t24_aa_simulation_runner removed — tables.md=speculation (simulation-mode branch only,
--   not live enquiry path); non-canonical; sim_run_date col dropped.

WITH
-- §Part1/step9 EM.DL.APPLICATION enrichment: dedup to latest version per dl_appl_id (curr_no DESC).
-- logic.md §Part1/step9: EM.EC.AA.GET.LOAN.TERM -> DlApplication_CacheRead when SHOW.FROM.TERM.AMOUNT.YN=N.
-- tables.md: EM.DL.APPLICATION role=enrichment, confidence=high.
-- Key: dl_appl_id from arr_customer_dedup (COALESCE(DL.APPL.ID) path, AA.ARR.CUSTOMER property).
-- Output column: dl_app_date (application date — DATE field on t24_em_dl_application).
em_dl_dedup AS (
    SELECT recid, app_date FROM (
        SELECT
            TRIM(CAST(recid AS VARCHAR))                                     AS recid,
            CAST(app_date AS DATE)                                           AS app_date,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(CAST(recid AS VARCHAR))
                ORDER BY curr_no DESC
            )                                                                AS rn
        FROM hive.bronze.t24_em_dl_application AT SNAPSHOT '{{ var("snap_em_dl_application") }}'
    ) t WHERE rn = 1
),
-- GRAIN-FANOUT FIX (reconcile 2026-06-23): t24_em_lo_application is a versioned dim
-- (33,785 rows / 32,785 distinct recid -> 1,000 dup recids). Joined RAW it multiplied the
-- per-arrangement grain (silver 18,585 rows vs 17,955 distinct arrangement = 630 phantom rows).
-- Dedup to latest version per recid (curr_no DESC) so first_pay_date stays 1:1.
em_lo_dedup AS (
    SELECT recid, rep_start_date FROM (
        SELECT
            TRIM(CAST(recid AS VARCHAR))                                     AS recid,
            rep_start_date,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(CAST(recid AS VARCHAR))
                ORDER BY curr_no DESC
            )                                                                AS rn
        FROM hive.bronze.t24_em_lo_application AT SNAPSHOT '{{ var("snap_em_lo_application") }}'
    ) t WHERE rn = 1
),
-- AA.ARR.CUSTOMER property-class records keyed <ARR.ID>-CUSTOMER-<DATE>.<seq>.
-- Carries DL.APPL.ID / LOAN.APPL.ID / GROUP.ID for APPL.ID COALESCE and GROUP chain.
-- Reduce to one row per arrangement (latest effective-date component in recid part 3).
arr_customer_dedup AS (
    SELECT arr_id, dl_appl_id, loan_appl_id, group_id FROM (
        SELECT
            SPLIT_PART(CAST(recid AS VARCHAR), '-', 1)                       AS arr_id,
            TRIM(CAST(dl_appl_id AS VARCHAR))                                AS dl_appl_id,
            TRIM(CAST(loan_appl_id AS VARCHAR))                              AS loan_appl_id,
            TRIM(CAST(group_id AS VARCHAR))                                  AS group_id,
            ROW_NUMBER() OVER (
                PARTITION BY SPLIT_PART(CAST(recid AS VARCHAR), '-', 1)
                ORDER BY SPLIT_PART(SPLIT_PART(CAST(recid AS VARCHAR), '-', 3), '.', 1) DESC
            )                                                                AS rn
        FROM hive.bronze.t24_aa_arr_customer AT SNAPSHOT '{{ var("snap_aa_arr_customer") }}'
    ) t WHERE rn = 1
),
-- AA.ARR.TERM.AMOUNT property-class records keyed <ARR.ID>-TERM.AMOUNT-<DATE>.<seq>.
-- TERM extracted from field 5 (term) of the TERM.AMOUNT property (spec Field 25, param-Y path).
arr_term_dedup AS (
    SELECT arr_id, term FROM (
        SELECT
            SPLIT_PART(CAST(recid AS VARCHAR), '-', 1)                       AS arr_id,
            TRIM(CAST(term AS VARCHAR))                                      AS term,
            ROW_NUMBER() OVER (
                PARTITION BY SPLIT_PART(CAST(recid AS VARCHAR), '-', 1)
                ORDER BY SPLIT_PART(SPLIT_PART(CAST(recid AS VARCHAR), '-', 3), '.', 1) DESC
            )                                                                AS rn
        FROM hive.bronze.t24_aa_arr_term_amount AT SNAPSHOT '{{ var("snap_aa_arr_term_amount") }}'
        WHERE CAST(recid AS VARCHAR) LIKE '%-TERM.AMOUNT-%'
    ) t WHERE rn = 1
),
-- AA.ACCOUNT.DETAILS keyed by arrangement @ID (one record per arrangement).
-- payment_start_date = fallback for FIRST.PAY.DATE when no EM.LO.APPLICATION record exists.
acct_details AS (
    SELECT
        TRIM(CAST(recid AS VARCHAR))                                         AS arr_id,
        payment_start_date,
        maturity_date
    FROM hive.bronze.t24_aa_account_details AT SNAPSHOT '{{ var("snap_aa_account_details") }}'
),
-- CUSTOMER dim dedup: keep latest version per recid so owner-name join stays 1:1.
-- name_1 is a LIST bronze column → access name_1[0].
-- RECURRING BUG FIX: ORDER BY curr_no DESC (not recid string) to get the latest version.
customer_dedup AS (
    SELECT recid, name_1 FROM (
        SELECT recid, name_1,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_customer AT SNAPSHOT '{{ var("snap_customer") }}'
    ) t WHERE rn = 1
),
-- §12 MCB.GROUP (wired 2026-06-19): group name via arr_customer.group_id (dev-NULL: group_id empty).
-- ORDER BY recid DESC: curr_no is absent in hive.bronze.t24_mcb_group (verified via INFORMATION_SCHEMA 2026-06-20).
mcb_dedup AS (
    SELECT recid, g_name FROM (
        SELECT recid, g_name, ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR)) ORDER BY recid DESC) AS rn
        FROM hive.bronze.t24_mcb_group
    WHERE business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
),
-- §12 Field 11 AA.CUSTOMER.ROLE description (wired 2026-06-19; 34 role records; description is LIST).
-- ORDER BY recid DESC: curr_no is absent in hive.bronze.t24_aa_customer_role AT SNAPSHOT '{{ var("snap_aa_customer_role") }}' (verified via INFORMATION_SCHEMA 2026-06-20).
role_dedup AS (
    SELECT recid, description FROM (
        SELECT recid, description[0] AS description,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR)) ORDER BY recid DESC) AS rn
        FROM hive.bronze.t24_aa_customer_role AT SNAPSHOT '{{ var("snap_aa_customer_role") }}'
    ) t WHERE rn = 1
),
-- §12 Fields 13-14 AA.PRODUCT description (wired): PRODUCT.DESC via L AA.PRODUCT,DESCRIPTION.
-- Join key: t24_aa_product.recid = aaa.product_group (confirmed in sibling t24_aa_wof_loans_report.sql).
-- description is a LIST column -> description[0].
-- ORDER BY recid DESC: curr_no is absent in hive.bronze.t24_aa_product (verified via INFORMATION_SCHEMA 2026-06-20).
product_dedup AS (
    SELECT recid, product_desc FROM (
        SELECT
            TRIM(CAST(recid AS VARCHAR))                                         AS recid,
            TRIM(CAST(description[0] AS VARCHAR))                                AS product_desc,
            ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR)) ORDER BY recid DESC) AS rn
        FROM hive.bronze.t24_aa_product
    WHERE business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
),
-- §12 Coverage field: AA.ARR.TERM.AMOUNT COMMITMENT records → commitment_amt.
-- Mirrors the COMMITMENT CTE in stg_credit_disbursement_t24 (§2 field 11 proxy).
-- AA.ARR.TERM.AMOUNT is in tables.md (access, high confidence).
-- NOTE: This is NOT the same as TOTCOMMBAL (EM.EC.AA.GET.TYPE.BAL 'TOTCOMMITMENT' from EB.CONTRACT.BALANCES
--       which remains NULL/unresolvable). This is the originally-approved COMMITMENT amount property.
commitment_dedup AS (
    SELECT arr_id, commitment_amt FROM (
        SELECT
            SPLIT_PART(CAST(recid AS VARCHAR), '-', 1)                       AS arr_id,
            CAST(amount AS DECIMAL(18,2))                                    AS commitment_amt,
            ROW_NUMBER() OVER (
                PARTITION BY SPLIT_PART(CAST(recid AS VARCHAR), '-', 1)
                ORDER BY SPLIT_PART(SPLIT_PART(CAST(recid AS VARCHAR), '-', 3), '.', 1) DESC
            )                                                                AS rn
        FROM hive.bronze.t24_aa_arr_term_amount AT SNAPSHOT '{{ var("snap_aa_arr_term_amount") }}'
        WHERE CAST(recid AS VARCHAR) LIKE '%-COMMITMENT-%'
    ) t WHERE rn = 1
),
-- §12 Fields 30-31 arrears: AA.BILL.DETAILS aggregated to 1 row per arrangement (wired 2026-06-19; 62 match).
bill_agg AS (
    SELECT TRIM(CAST(arrangement_id AS VARCHAR)) AS arrangement_id,
           SUM(CAST(os_total_amount AS DECIMAL(18,2))) AS overdue_amt
    FROM hive.bronze.t24_aa_bill_details AT SNAPSHOT '{{ var("snap_aa_bill_details") }}'
    GROUP BY TRIM(CAST(arrangement_id AS VARCHAR))
)

SELECT
    -- ARRANGEMENT (spec Field 3) — driving file @id
    LPAD(TRIM(CAST(aaa.recid AS VARCHAR)), 17, ' ')                                              AS arrangement,
    -- APPL.ID (spec Field 7) — COALESCE(DL.APPL.ID, LOAN.APPL.ID) from AA.ARR.CUSTOMER property-class
    -- spec: "IF DL.APPL.ID NE EMPTY DL.APPL.ID LOAN.APPL.ID"
    COALESCE(
        NULLIF(ac.dl_appl_id, ''),
        ac.loan_appl_id
    )                                                                                            AS appl_id,
    -- CO.CODE / COMPANY-BRANCH (spec Field 8)
    TRIM(CAST(aaa.co_code AS VARCHAR))                                                           AS co_code,
    -- ACCOUNT (spec Field 9) — first value of multi-value LINKED.APPL.ID (Conversion V 1, Single Multi M)
    TRIM(CAST(aaa.linked_appl_id[0] AS VARCHAR))                                                 AS account,
    -- CUSTOMER (spec Field 10) — multi-value displayed as value-mark list; first value (LIST)
    TRIM(CAST(aaa.customer[0] AS VARCHAR))                                                       AS customer,
    -- CUSTOMER.ROLE (spec Field 11) L AA.CUSTOMER.ROLE,DESCRIPTION on arrangement.customer_role[0]
    role.description                                                                             AS customer_role,
    -- ACCOUNT.OFFICER (spec Field 12) — not present on t24_aa_arrangement in bronze; canonical DAO
    -- source column absent from in-scope tables -> honest NULL (no out-of-scope table used).
    CAST(NULL AS VARCHAR)                                                                        AS account_officer,
    -- PRODUCT.DESC / PRODUCT NAME (spec Fields 13-14) — L AA.PRODUCT,DESCRIPTION on arrangement.product_group.
    -- Join: product_dedup.recid = TRIM(aaa.product_group) (confirmed via sibling t24_aa_wof_loans_report).
    prd.product_desc                                                                             AS product_desc,
    -- GROUP.ID (spec Fields 15-16) from AA.ARR.CUSTOMER property-class
    ac.group_id                                                                                  AS group_id,
    -- GROUP.NAME (spec Field 17) L MCB.GROUP,G.NAME via arr_customer.group_id
    mcb.g_name                                                                                   AS group_name,
    -- OWNER (spec Field 18) — owner customer ids = AA.ARRANGEMENT.CUSTOMER; first value (LIST)
    TRIM(CAST(aaa.customer[0] AS VARCHAR))                                                       AS owner,
    -- OWNER.NAME / CUSTOMER NAME (spec Field 19) L CUSTOMER,NAME.1 on OWNER (LIST name_1[0])
    TRIM(CAST(cus.name_1[0] AS VARCHAR))                                                         AS owner_name,
    -- START.DATE / OPENING DATE (spec Field 20) — real DATE from AA.ARRANGEMENT
    CAST(aaa.start_date AS DATE)                                                                 AS start_date,
    -- FIRST.PAY.DATE (spec Field 23) — COALESCE(LO.REP.START.DATE, AA.ACCOUNT.DETAILS.PAYMENT.START.DATE)
    -- spec: "IF LO.PAY.START NE '' LO.PAY.START AA.PAY.START"; lo joined via recid = ac.loan_appl_id
    COALESCE(CAST(lo.rep_start_date AS DATE), CAST(acd.payment_start_date AS DATE))             AS first_pay_date,
    -- MATURITY.DATE (spec Field 24) — real DATE
    CAST(acd.maturity_date AS DATE)                                                              AS maturity_date,
    -- TERM (spec Field 25) — param-Y path: AA.ARR.TERM.AMOUNT field 5 (term)
    atm.term                                                                                     AS term,
    -- CURRENCY / CCY (spec Field 26)
    TRIM(CAST(aaa.currency AS VARCHAR))                                                          AS currency,
    -- CATEGORY_CODE (disbursement coverage field) — t24_account.category not in tables.md → honest NULL.
    -- No in-scope table exposes the numeric category code (3001-3028) for this enquiry object.
    CAST(NULL AS VARCHAR)                                                                        AS category_code,
    -- COMMITMENT_AMT (disbursement coverage field) — approved commitment amount from AA.ARR.TERM.AMOUNT
    -- COMMITMENT records (tables.md: access, high). Best-effort proxy for total_disbursement used in
    -- stg_credit_disbursement_t24. Distinct from TOTCOMMBAL (EB.CONTRACT.BALANCES, still NULL below).
    cmt.commitment_amt                                                                           AS commitment_amt,
    -- TOTCOMMBAL / COMMITMENT (spec Field 27) — EM.EC.AA.GET.TYPE.BAL('TOTCOMMITMENT');
    -- physical AA balance store (EB.CONTRACT.BALANCES) unresolved → NULL
    CAST(NULL AS DECIMAL(18,2))                                                                  AS totcommbal,
    -- CURRACCBAL / PRINCIPAL (spec Fields 28-29) — EM.EC.AA.GET.TYPE.BAL('CUR'+prop);
    -- physical AA balance store unresolved → NULL
    CAST(NULL AS DECIMAL(18,2))                                                                  AS curraccbal,
    -- DUE.TODAY (spec Field 30) — EM.EC.CALCULATE.ARR.ARREARS procedural over AA.BILL.DETAILS → NULL
    CAST(NULL AS DECIMAL(18,2))                                                                  AS due_today,
    -- OVERDUE (spec Field 31) — AA.BILL.DETAILS os_total_amount summed per arrangement (wired 2026-06-19)
    bill.overdue_amt                                                                             AS overdue,
    -- RESCH.IND (spec Field 32) — EM.EC.AA.GET.RESCHEDULE.INFO routine source unavailable → NULL
    CAST(NULL AS VARCHAR)                                                                        AS resch_ind,
    -- ARR.STATUS.DISP / STATUS (spec Field 42) — activity-history table unresolved;
    -- product-code ARR.STATUS CASE-chain fallback (spec Fields 34-41, LENDING/AL mapping)
    CASE TRIM(CAST(aaa.arr_status AS VARCHAR))
        WHEN 'AUTH'            THEN 'Not Disbursed'
        WHEN 'CURRENT'         THEN 'Current'
        WHEN 'EXPIRED'         THEN 'Expired'
        WHEN 'PENDING.CLOSURE' THEN 'Pending Closure'
        WHEN 'UNAUTH'          THEN 'Unauthorised'
        ELSE TRIM(CAST(aaa.arr_status AS VARCHAR))
    END                                                                                          AS arr_status_disp,
    -- DL.APP.DATE — EM.DL.APPLICATION.APP.DATE (application date for the DL appl chain).
    -- logic.md §Part1/step9: DlApplication_CacheRead keyed on DL.APPL.ID; tables.md=high.
    -- Key: dl_appl_id from arr_customer_dedup (COALESCE DL path on AA.ARR.CUSTOMER property).
    -- NULL-honest: rows where dl_appl_id is empty will resolve to NULL (LEFT JOIN miss).
    dl.app_date                                                                                  AS dl_app_date,
    CAST(date '{{ var("business_date") }}' AS DATE)                AS business_date
FROM hive.bronze.t24_aa_arrangement AT SNAPSHOT '{{ var("snap_aa_arrangement") }}' aaa
LEFT JOIN arr_customer_dedup ac   ON ac.arr_id    = TRIM(CAST(aaa.recid AS VARCHAR))
LEFT JOIN arr_term_dedup     atm  ON atm.arr_id   = TRIM(CAST(aaa.recid AS VARCHAR))
LEFT JOIN acct_details       acd  ON acd.arr_id   = TRIM(CAST(aaa.recid AS VARCHAR))
LEFT JOIN customer_dedup     cus  ON TRIM(CAST(cus.recid AS VARCHAR)) = TRIM(CAST(aaa.customer[0] AS VARCHAR))
LEFT JOIN em_lo_dedup lo
       ON TRIM(CAST(lo.recid AS VARCHAR)) = ac.loan_appl_id
LEFT JOIN mcb_dedup mcb ON ac.group_id = TRIM(CAST(mcb.recid AS VARCHAR))
LEFT JOIN role_dedup role ON TRIM(CAST(aaa.customer_role[0] AS VARCHAR)) = TRIM(CAST(role.recid AS VARCHAR))
LEFT JOIN product_dedup prd ON TRIM(CAST(aaa.product_group AS VARCHAR)) = prd.recid
LEFT JOIN commitment_dedup cmt ON cmt.arr_id      = TRIM(CAST(aaa.recid AS VARCHAR))
LEFT JOIN bill_agg   bill ON bill.arrangement_id = TRIM(CAST(aaa.recid AS VARCHAR))
-- §Part1/step9 DL application: keyed on dl_appl_id from arr_customer_dedup (DL.APPL.ID path).
-- DEDUPED to 1 row per dl recid via em_dl_dedup CTE (curr_no DESC). No fan-out risk.
LEFT JOIN em_dl_dedup dl ON NULLIF(TRIM(ac.dl_appl_id), '') = dl.recid
WHERE TRIM(CAST(aaa.product_line AS VARCHAR)) = 'LENDING'
  -- Fixed selection (canonical UNCONDITIONAL): keep active/current arrangements only.
  AND TRIM(CAST(aaa.arr_status AS VARCHAR)) NOT IN ('UNAUTH', 'CANCELLED', 'CLOSE', 'MATURED')
ORDER BY aaa.start_date DESC, appl_id ASC
