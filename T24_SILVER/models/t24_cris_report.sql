{{ config(
    materialized='table',
    database='hive',
    schema='silver'
) }}

-- Division: credit | Report: §2 CRIS.REPORT (t24-built SILVER source table)
-- SILVER-zone t24-built equivalent of the "BNCTL Loan Provision Extract" DFE.
-- Emits the FULL Part-1 §2 field set: 29 output positions (LoanID ... MobilePhone),
-- one row per qualifying PV.ASSET.DETAIL lending asset
-- (per Part-1 §2 CRIS.REPORT, T24_REPORT_DEVELOPMENT_SOT.md).
--
-- §2 File Selection (mandatory, lines 1 & 2): PRODUCT.LINE='LENDING' AND CUSTOMER<>''.
-- This is the ONLY canonical selection. arr_status (ARR.STATUS) is a canonical OUTPUT field
-- (arrangement_status), NOT a selection filter — the prior arr_status='CURRENT' var-gated
-- filter was non-canonical and has been removed to match the canonical logic exactly.
--
-- Bronze conventions applied:
--   * LIST/multi-value bronze columns -> element access col[0] (NOT "col/0").
--   * occupation on t24_customer is an ARRAY -> occupation[0].
--   * AA property table (t24_aa_arr_payment_schedule) keyed by
--     composite recid <ARR.ID>-<PROPERTY>-<DATE>.<seq> -> join SPLIT_PART(recid,'-',1)=arr_id;
--     aggregated to one row per arrangement so the 1-row-per-asset grain is preserved.
--   * Versioned dims (t24_category, t24_customer, t24_dept_acct_officer) deduped via ROW_NUMBER()=1 (latest version, curr_no DESC).
--   * numeric id columns -> CAST(CAST(x AS BIGINT) AS VARCHAR) before LPAD (avoid trailing .0).
--   * Bronze date columns are real DATE -> CAST(x AS DATE) (never DATE_FORMAT).
WITH schedule_sum AS (
    -- §2 f15 RepaymentAmount: SUM(PsCalcAmount) over the PAYMENT.SCHEDULE property,
    -- one row per arrangement (aggregated -> no fan-out of the per-asset grain).
    -- calc_amount is a 2-element LIST: element [0] is empty for 100% of rows; the PsCalcAmount
    -- value sits in element [1] (90.6% populated in this property table). Use [1] not [0].
    -- REFERENTIAL-GAP NOTE (reconcile 2026-06-23): schedule_sum yields 4,927 arrangements (4,339 nonzero),
    -- but ALL 4,927 schedule arr_ids are DISJOINT from t24_account.arrangement_id (0/4,927 overlap; they
    -- live in the same AA namespace as aa_account_details.recid yet none are linked to an account in this
    -- snapshot). So the join below (ss.arr_id = acc.arrangement_id) matches 0 rows -> repayment_amount
    -- resolves to COALESCE(...,0)=0 for 100% of cris output rows. This is `t24 tables cannot join`
    -- (snapshot referential gap), NOT a mapping bug — join logic is canonical per Registry §5.
    -- Downstream final_repayment_report.last_instalment is therefore 0 for all rows (honest data gap).
    SELECT
        SPLIT_PART(recid, '-', 1)                                    AS arr_id,
        SUM(CAST(calc_amount[1] AS DECIMAL(18,2)))                   AS rep_amount
    FROM hive.bronze.t24_aa_arr_payment_schedule AT SNAPSHOT '{{ var("snap_aa_arr_payment_schedule") }}'
    WHERE recid LIKE '%-SCHEDULE-%'
    GROUP BY SPLIT_PART(recid, '-', 1)
),
-- Dimension dedup: bronze t24_category / t24_customer carry multiple versioned rows per
-- recid -> would fan out the 1-row-per-asset grain. Keep the latest version per recid.
category_dedup AS (
    -- t24_category: versioned dim -> dedup to latest version per recid using curr_no DESC.
    -- short_name is a LIST -> [0].
    SELECT recid, short_name FROM (
        SELECT recid, short_name[0] AS short_name,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_category
    WHERE business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
),
customer_dedup AS (
    -- t24_customer: versioned dim -> dedup to latest version per recid using curr_no DESC.
    -- Column is id_types (with s), not id_type.
    -- name_1 (NAME.1) + name_2 (NAME.2) are LIST cols -> [0] at use site; carried for CustomerName / CustomerShortName.
    -- sector (CUSTOMER.SECTOR) carried for CreditbySector enrichment (mapped via map_sector_credit seed).
    SELECT recid, account_officer, id_types, legal_id, employ_name,
           occupation, sp_name, tel_mobile, name_1, name_2, sector FROM (
        SELECT recid, account_officer, id_types, legal_id, employ_name,
               occupation, sp_name, tel_mobile, name_1, name_2, sector,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_customer AT SNAPSHOT '{{ var("snap_customer") }}'
    ) t WHERE rn = 1
),
-- t24_dept_acct_officer exists in bronze (wired 2026-06-19); officer_dedup CTE below resolves
-- DEPT.ACCT.OFFICER.NAME via CUSTOMER.ACCOUNT.OFFICER -> DEPT.ACCT.OFFICER.@ID (field 5).
aa_details_dedup AS (
    -- §2 f13 MaturityDate + f14 ArrangementStatus: AA.ACCOUNT.DETAILS keyed by ARRANGEMENT.ID.
    -- maturity_date: canonical AA.ACCOUNT.DETAILS.MATURITY.DATE.
    -- arr_age_status: canonical ARR.STATUS (arr_status) is NOT materialized in bronze t24_aa_account_details;
    --   ARR.AGE.STATUS (arr_age_status) is the only populated arrangement-level status in this canonical
    --   table (99.2% populated: CUR/NAB/...; verified vs bronze 2026-06-22). Used as ArrangementStatus
    --   source — same canonical table, NOT the rejected foreign AA.ARRANGEMENT route.
    SELECT recid, maturity_date, arr_age_status FROM (
        SELECT recid, maturity_date, arr_age_status,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY TRIM(CAST(recid AS VARCHAR)) DESC) AS rn
               -- NOTE: t24_aa_account_details has no curr_no column in bronze (confirmed via
               -- INFORMATION_SCHEMA 2026-06-20); latest-version selection via curr_no DESC is
               -- unavailable. Falling back to recid-based tiebreak (documented exception).
        FROM hive.bronze.t24_aa_account_details AT SNAPSHOT '{{ var("snap_aa_account_details") }}'
    ) t WHERE rn = 1
),
officer_dedup AS (
    -- §2 f5 AccountOfficer name: DEPT.ACCT.OFFICER (exists in bronze; versioned dim -> curr_no DESC).
    SELECT recid, name FROM (
        SELECT recid, name,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                                  ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_dept_acct_officer
    WHERE business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
),
sector_map AS (
    -- CreditbySector enrichment map: CUSTOMER.SECTOR code -> regulatory sector bucket.
    -- Seed (seeds/map_sector_credit.csv). Unmapped codes -> NULL -> downstream COALESCE(...,10) = 'Others'.
    SELECT CAST(sector_code AS VARCHAR)      AS sector_code,
           CAST(credit_by_sector AS INTEGER) AS credit_by_sector
    FROM hive.silver.map_sector_credit
),
-- 11 Commitment — RECONCILE 2026-06-23: was CAST(NULL) (stub claimed AA.ARR.TERM.AMOUNT "foreign / no
-- bronze column"). That was WRONG: hive.bronze.t24_aa_arr_term_amount AT SNAPSHOT '{{ var("snap_aa_arr_term_amount") }}' IS in bronze and the sibling
-- silver t24_find_arrangement_al_bnctl already sources COMMITMENT from it. Wired here using the same
-- recipe: COMMITMENT property records (recid '<ARR.ID>-COMMITMENT-<DATE>.<seq>'), amount field, latest
-- version per arrangement. Joined on arrangement_id. Populates ~6,990/41,196 cris loans; rest = no
-- COMMITMENT record for that arrangement in the snapshot (data gap).
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
)

SELECT
    -- 1 (id) LoanID  — PV.ASSET.DETAIL.@ID; numeric id -> BIGINT->VARCHAR before LPAD (avoid .0)
    LPAD(CAST(CAST(pad.recid AS BIGINT) AS VARCHAR), 14, '0')                                        AS loan_id,
    -- 2  Category — L ACCOUNT,CATEGORY
    CAST(acc.category AS INTEGER)                                                                    AS category,
    -- 3  ProductName — two-hop ACCOUNT->CATEGORY->SHORT.NAME (LIST col[0])
    TRIM(CAST(cat.short_name AS VARCHAR))                                                            AS product_name,
    -- 4  CustomerNumber — PV.ASSET.DETAIL.CUSTOMER (pos 6)
    TRIM(CAST(pad.customer AS VARCHAR))                                                              AS customer_number,
    -- 5  AccountOfficer — CUSTOMER.ACCOUNT.OFFICER -> DEPT.ACCT.OFFICER.NAME (wired 2026-06-19)
    TRIM(CAST(dao.name AS VARCHAR))                                                                  AS account_officer,
    -- 6  Currency — PV.ASSET.DETAIL.CURRENCY (pos 2), pass-through
    TRIM(CAST(pad.currency AS VARCHAR))                                                              AS currency,
    -- 7  AutoClass — PV.ASSET.DETAIL.AUTO.CLASS (pos 15)
    TRIM(CAST(pad.auto_class AS VARCHAR))                                                            AS auto_class,
    -- 8  LoanOutstanding — L ACCOUNT,WORKING.BALANCE
    CAST(acc.working_balance AS DECIMAL(18,2))                                                       AS loan_outstanding,
    -- 9  ProvisionType — PV.ASSET.DETAIL.CALC.PROV.TYPE (multi-value; first element)
    TRIM(CAST(pad.calc_prov_type[0] AS VARCHAR))                                                     AS provision_type,
    -- 10 BaseAmount — PV.ASSET.DETAIL.CALC.BASE.AMT (multi-value; first element)
    CAST(pad.calc_base_amt[0] AS DECIMAL(18,2))                                                      AS base_amount,
    -- 11 Commitment — AA.ARR.TERM.AMOUNT COMMITMENT property amount (wired 2026-06-23; see commitment_dedup CTE).
    cmt.commitment_amt                                                                              AS commitment,
    -- 12 Term — derived @EM.XX.DFE.CONV.GET.TERM (AA property; no materialized bronze column)
    CAST(NULL AS INTEGER)                                                                            AS term,
    -- 13 MaturityDate — AA.ACCOUNT.DETAILS.MATURITY.DATE (real DATE)
    CAST(aad.maturity_date AS DATE)                                                                  AS maturity_date,
    -- 14 ArrangementStatus — AA.ACCOUNT.DETAILS.ARR.AGE.STATUS (arr_age_status).
    --    Canonical ARR.STATUS (arr_status) absent from bronze; ARR.AGE.STATUS is the only populated
    --    arrangement-level status in the same canonical table (99.2%: CUR/NAB/...; verified 2026-06-22).
    TRIM(CAST(aad.arr_age_status AS VARCHAR))                                                        AS arrangement_status,
    -- 15 RepaymentAmount — SUM(PsCalcAmount) PAYMENT.SCHEDULE; ZERO.IF.EMPTY (x2) -> COALESCE 0
    COALESCE(ss.rep_amount, CAST(0 AS DECIMAL(18,2)))                                                AS repayment_amount,
    -- 16 LastDateCr — L ACCOUNT,DATE.LAST.CR.AUTO (real DATE)
    CAST(acc.date_last_cr_auto AS DATE)                                                              AS last_date_cr,
    -- 17 OpeningDate — L ACCOUNT,OPENING.DATE (real DATE)
    CAST(acc.opening_date AS DATE)                                                                   AS opening_date,
    -- 18 IDType — L CUSTOMER,ID.TYPE (bronze column is id_types, with s)
    TRIM(CAST(cus.id_types AS VARCHAR))                                                              AS id_type,
    -- 19 CustomerName — L CUSTOMER,NAME.1 (LIST col[0]); full name line per client mapping.
    --    (bronze full_name col exists but is 0% populated; name_1 is 100% populated -> use name_1)
    TRIM(CAST(cus.name_1[0] AS VARCHAR))                                                             AS customer_name,
    -- 19b CustomerShortName — L CUSTOMER,NAME.2 (LIST col[0]); extra column per client request
    TRIM(CAST(cus.name_2[0] AS VARCHAR))                                                             AS customer_short_name,
    -- 20 CompanyCode — PV.ASSET.DETAIL.CO.CODE (pos 84)
    TRIM(CAST(pad.co_code AS VARCHAR))                                                               AS company_code,
    -- 21 ArrangementID — L ACCOUNT,ARRANGEMENT.ID
    TRIM(CAST(acc.arrangement_id AS VARCHAR))                                                        AS arrangement_id,
    -- 22 LegalID — L CUSTOMER,LEGAL.ID (LIST col[0])
    TRIM(CAST(cus.legal_id[0] AS VARCHAR))                                                           AS legal_id,
    -- 23 EmployerName — L CUSTOMER,EMPLOY.NAME
    TRIM(CAST(cus.employ_name AS VARCHAR))                                                           AS employer_name,
    -- 24 InterestRate — derived @EM.XX.DFE.CONV.GET.INT.RATE (AA property; no bronze column)
    CAST(NULL AS DECIMAL(18,6))                                                                      AS interest_rate,
    -- 25 CurrentInterest — derived @EM.XX.DFE.CONV.LOAN.CURR.INTEREST (routine; no bronze column)
    CAST(NULL AS DECIMAL(18,2))                                                                      AS current_interest,
    -- 26 AvailablePrincipal — derived @EM.XX.DFE.CONV.LOAN.AVAL.PRINC (routine; no bronze column)
    CAST(NULL AS DECIMAL(18,2))                                                                      AS available_principal,
    -- 27 Occupation — L CUSTOMER,OCCUPATION (ARRAY -> occupation[0]); implicit @ID->ACCOUNT->CUSTOMER
    TRIM(CAST(cus.occupation[0] AS VARCHAR))                                                         AS occupation,
    -- 28 SpouseName — L CUSTOMER,SP.NAME
    TRIM(CAST(cus.sp_name AS VARCHAR))                                                               AS spouse_name,
    -- 29 MobilePhone — L CUSTOMER,TEL.MOBILE
    TRIM(CAST(cus.tel_mobile AS VARCHAR))                                                            AS mobile_phone,
    -- current-source coverage fields (cris_mock.csv columns not in CRIS.REPORT DFE logic.md)
    -- These enrichment columns are downstream-derived (category CASE logic in stg layer);
    -- CRIS.REPORT DFE does not define them -> honest NULL per absent-from-logic rule.
    -- 30 NameCreditGrantor — not in logic.md / tables.md; no bronze column
    CAST(NULL AS VARCHAR)                                                                            AS name_credit_grantor,
    -- 31 CreditbySector — CUSTOMER.SECTOR (cus.sector, 98.8% pop) mapped via map_sector_credit seed.
    --    Nullable when customer unmatched OR sector code not in seed -> downstream COALESCE(...,10)='Others'.
    sm.credit_by_sector                                                                             AS credit_by_sector,
    -- 32 MannerofPaymt — not in logic.md; downstream category-CASE enrichment
    CAST(NULL AS VARCHAR)                                                                            AS manner_of_paymt,
    -- 33 Security — not in logic.md; downstream category-CASE enrichment
    CAST(NULL AS VARCHAR)                                                                            AS security,
    -- 34 DescofCollateral — not in logic.md; downstream category-CASE enrichment
    CAST(NULL AS VARCHAR)                                                                            AS desc_of_collateral,
    CAST(date '{{ var("business_date") }}' AS DATE)                AS business_date
FROM hive.bronze.t24_pv_asset_detail AT SNAPSHOT '{{ var("snap_pv_asset_detail") }}' pad
JOIN      hive.bronze.t24_account AT SNAPSHOT '{{ var("snap_account") }}'     acc ON TRIM(CAST(pad.recid AS VARCHAR)) = TRIM(CAST(acc.recid AS VARCHAR))
-- two-hop ACCOUNT.CATEGORY -> CATEGORY.@ID -> SHORT.NAME (numeric id -> VARCHAR to match recid)
LEFT JOIN category_dedup    cat ON CAST(CAST(acc.category AS INTEGER) AS VARCHAR) = TRIM(CAST(cat.recid AS VARCHAR))
-- §2 chain step 3: PV.ASSET.DETAIL.CUSTOMER -> CUSTOMER.@ID (covers fields 18/22/23/27/28/29)
LEFT JOIN customer_dedup    cus ON TRIM(CAST(pad.customer AS VARCHAR)) = TRIM(CAST(cus.recid AS VARCHAR))
-- §2 chain step 4: CUSTOMER.ACCOUNT.OFFICER -> DEPT.ACCT.OFFICER.@ID -> NAME (field 5)
LEFT JOIN officer_dedup     dao ON TRIM(CAST(cus.account_officer AS VARCHAR)) = TRIM(CAST(dao.recid AS VARCHAR))
-- CreditbySector enrichment: CUSTOMER.SECTOR -> map_sector_credit seed -> regulatory bucket
LEFT JOIN sector_map        sm  ON TRIM(CAST(cus.sector AS VARCHAR)) = sm.sector_code
-- §2 chain step 5: ACCOUNT.ARRANGEMENT.ID -> AA.ACCOUNT.DETAILS.@ID -> MATURITY.DATE + ARR.STATUS
LEFT JOIN aa_details_dedup    aad ON TRIM(CAST(acc.arrangement_id AS VARCHAR)) = TRIM(CAST(aad.recid AS VARCHAR))
-- §2 chain step 6: ACCOUNT.ARRANGEMENT.ID -> AA payment-schedule property
LEFT JOIN schedule_sum      ss  ON ss.arr_id = TRIM(CAST(acc.arrangement_id AS VARCHAR))
-- 11 Commitment: AA.ARR.TERM.AMOUNT COMMITMENT property amount, joined on arrangement_id (wired 2026-06-23)
LEFT JOIN commitment_dedup  cmt ON cmt.arr_id = TRIM(CAST(acc.arrangement_id AS VARCHAR))
WHERE TRIM(CAST(pad.product_line AS VARCHAR)) = 'LENDING'
  AND pad.customer IS NOT NULL AND TRIM(CAST(pad.customer AS VARCHAR)) <> ''
