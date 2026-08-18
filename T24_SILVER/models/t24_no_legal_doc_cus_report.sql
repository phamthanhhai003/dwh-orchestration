{{ config(
    materialized='table',
    database='hive',
    schema='silver'
) }}

-- Division: credit/compliance | Object: NO.LEGAL.DOC.CUS.REPORT (t24-built source)
-- Reads real hive.bronze.t24_* per Part-1 §14 NO.LEGAL.DOC.CUS.REPORT (T24_REPORT_DEVELOPMENT_SOT.md).
--
-- Driver: CUSTOMER, selected when legal doc is expired (LEGAL.EXP.DATE <= TODAY and not blank)
-- OR legal id number is blank. Grain = one row per qualifying ACCOUNT for each such customer.
-- Account-level category filter (hard-coded in routine): 6005/6006/6007/6010/1001/1002/1003.
--
-- Output is the assembled 10-element *-delimited row; element 1 (COMP.ID) is internal/suppressed
-- in the ENQ display but emitted here as comp_id. Displayed cols are F *,2,1 .. F *,10,1.
--
-- Bronze conventions (see stg__loan_sector_detail_t24.sql):
--   * LIST/multivalue bronze columns -> element access col[0] (NOT "col/0").
--   * Dedup versioned dims (t24_customer, t24_company): ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
--     ORDER BY curr_no DESC) = 1.
--   * numeric id columns -> CAST(CAST(x AS BIGINT) AS VARCHAR) before any LPAD (avoid .0).
--   * Bronze date columns are real DATE -> CAST(x AS DATE); never DATE_FORMAT.
--   * Qualifying filter is UNCONDITIONAL per canonical ENQ logic (var-gate removed 2026-06-19).
-- 2026-06-22 reconcile fix: legal_exp_date confirmed as ARRAY in bronze t24_customer; expired_date_doc
--   now maps to cus.legal_exp_date[0] (was CAST(NULL)). Expired-doc driver arm re-enabled in WHERE.
-- Wired (2026-06-21): t24_customer_account (FBNK_CUSTOMER_ACCOUNT) — logic.md §Part1/step10:
--   CUSTOMER.ACCOUNT MV list of account ids per customer; tables.md=high.
--   DEDUPED LEFT JOIN on customer recid; emits cust_account_ref (account_number field).

WITH customer_dedup AS (
    -- t24_customer: versioned dim -> keep latest per recid using curr_no DESC (not recid DESC — recurring bug fix).
    SELECT *
    FROM (
        SELECT
            c.*,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(CAST(c.recid AS VARCHAR))
                ORDER BY curr_no DESC
            ) AS rn
        FROM hive.bronze.t24_customer AT SNAPSHOT '{{ var("snap_customer") }}' c
    ) t
    WHERE rn = 1
),

company_dedup AS (
    -- t24_company: versioned dim -> dedup via curr_no DESC (2 rows per recid confirmed in siblings).
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

-- §Part1/step10 CUSTOMER.ACCOUNT: dedup to 1 row per customer recid.
-- logic.md §Part1/step10: "Read CUSTOMER.ACCOUNT for the customer to get the list of account ids".
-- tables.md: CUSTOMER.ACCOUNT role=access, confidence=high.
-- Schema: recid=customer_id, account_number=first MV account ref (VARCHAR).
-- DEDUP: no curr_no on t24_customer_account (3-col schema); ORDER BY recid DESC as tiebreak.
-- No fan-out: 1 row per customer recid in CTE → LEFT JOIN on cus.recid is 1:1.
cust_account_dedup AS (
    SELECT recid, account_number FROM (
        SELECT
            TRIM(CAST(recid AS VARCHAR))                                     AS recid,
            TRIM(CAST(account_number AS VARCHAR))                            AS account_number,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(CAST(recid AS VARCHAR))
                ORDER BY TRIM(CAST(recid AS VARCHAR)) DESC
            )                                                                AS rn
        FROM hive.bronze.t24_customer_account AT SNAPSHOT '{{ var("snap_customer_account") }}'
    ) t WHERE rn = 1
)

SELECT
    -- ══════════════════════════════════════════════════════════════════
    -- ENQ output fields (canonical F *,1,1 .. F *,10,1 per logic.md §12)
    -- ══════════════════════════════════════════════════════════════════

    -- element 1 (internal, not displayed): COMP.ID
    TRIM(CAST(acc.co_code AS VARCHAR))                                                       AS comp_id,
    -- F *,2,1  Branch Name (COMPANY keyed on account.co_code)
    TRIM(CAST(comp.company_name[0] AS VARCHAR))                                              AS branch_name,
    -- F *,3,1  Customer Number
    TRIM(CAST(cus.recid AS VARCHAR))                                                         AS customer_number,
    -- F *,4,1  Account Number
    TRIM(CAST(acc.recid AS VARCHAR))                                                         AS account_number,
    -- F *,5,1  Customer Name = TRIM(title + name_one + name_two + short_name)
    -- NOTE: cus.title is a plain VARCHAR scalar (confirmed via dbt show 2026-06-20); no [0] access needed.
    --       name_1, name_2, short_name are LIST columns -> [0] per convention.
    TRIM(
        CONCAT_WS(' ',
            NULLIF(TRIM(CAST(cus.title         AS VARCHAR)), ''),
            NULLIF(TRIM(CAST(cus.name_1[0]     AS VARCHAR)), ''),
            NULLIF(TRIM(CAST(cus.name_2[0]     AS VARCHAR)), ''),
            NULLIF(TRIM(CAST(cus.short_name[0] AS VARCHAR)), '')
        )
    )                                                                                        AS customer_name,
    -- F *,6,1  Date of Birth = COALESCE(date_of_birth, birth_incorp_date)
    CAST(COALESCE(cus.date_of_birth, cus.birth_incorp_date) AS DATE)                         AS date_of_birth,
    -- F *,7,1  Open Account Date
    CAST(acc.opening_date AS DATE)                                                           AS open_account_date,
    -- F *,8,1  Id Doc Type — EB.LOOKUP description (primary); GIC.ID description (fallback per logic.md §7); raw code (last resort)
    COALESCE(
        TRIM(CAST(luk1.lu_desc AS VARCHAR)),
        TRIM(CAST(g.gic_desc AS VARCHAR)),
        TRIM(CAST(cus.legal_doc_name[0] AS VARCHAR))
    )                                                                                        AS id_doc_type,
    -- F *,9,1  Id Doc No
    TRIM(CAST(cus.legal_id[0] AS VARCHAR))                                                   AS id_doc_no,
    -- F *,10,1 Expired Date Doc — legal_exp_date ARRAY confirmed in bronze t24_customer (2026-06-22 reconcile)
    -- legal_exp_date[0] is the first (current) expiry date for the customer's legal document.
    -- For blank-legal-id arm: value is NULL (these customers have no legal doc at all — by-design).
    -- For expired-doc arm: value is the actual expiry date.
    CAST(cus.legal_exp_date[0] AS DATE)                                                      AS expired_date_doc,

    -- ══════════════════════════════════════════════════════════════════
    -- Coverage additions — rpt_aml05 column targets (stg_aml05__customers aliases)
    -- ══════════════════════════════════════════════════════════════════

    -- customer_id: same value as customer_number (rpt_aml05 c.customer_id join key)
    TRIM(CAST(cus.recid AS VARCHAR))                                                         AS customer_id,

    -- customer_full_name: same derivation as customer_name (rpt_aml05 c.customer_full_name)
    -- NOTE: cus.title is a plain VARCHAR scalar (confirmed via dbt show 2026-06-20); no [0] access needed.
    TRIM(
        CONCAT_WS(' ',
            NULLIF(TRIM(CAST(cus.title         AS VARCHAR)), ''),
            NULLIF(TRIM(CAST(cus.name_1[0]     AS VARCHAR)), ''),
            NULLIF(TRIM(CAST(cus.name_2[0]     AS VARCHAR)), ''),
            NULLIF(TRIM(CAST(cus.short_name[0] AS VARCHAR)), '')
        )
    )                                                                                        AS customer_full_name,

    -- customer_short_name: t24_customer.short_name LIST -> [0] (rpt_aml05 c.customer_short_name)
    TRIM(CAST(cus.short_name[0] AS VARCHAR))                                                 AS customer_short_name,

    -- birth_or_incorp_date: alias of date_of_birth derivation (rpt_aml05 c.birth_or_incorp_date)
    CAST(COALESCE(cus.date_of_birth, cus.birth_incorp_date) AS DATE)                         AS birth_or_incorp_date,

    -- legal_id: alias of id_doc_no (rpt_aml05 c.legal_id)
    TRIM(CAST(cus.legal_id[0] AS VARCHAR))                                                   AS legal_id,

    -- legal_doc_type: alias of id_doc_type derivation (rpt_aml05 c.legal_doc_type)
    COALESCE(
        TRIM(CAST(luk1.lu_desc AS VARCHAR)),
        TRIM(CAST(g.gic_desc AS VARCHAR)),
        TRIM(CAST(cus.legal_doc_name[0] AS VARCHAR))
    )                                                                                        AS legal_doc_type,

    -- legal_doc_expiry_date: alias of expired_date_doc (legal_exp_date[0]); column confirmed in bronze 2026-06-22
    -- (rpt_aml05 c.legal_doc_expiry_date)
    CAST(cus.legal_exp_date[0] AS DATE)                                                      AS legal_doc_expiry_date,

    -- has_no_legal_id: derived flag — 'Y' when legal_id[0] is blank/null (rpt_aml05 c.has_no_legal_id)
    -- Mirrors the blank-id arm of the driver SELECT condition (logic.md §2/§9).
    CASE
        WHEN cus.legal_id IS NULL
          OR cus.legal_id[0] IS NULL
          OR TRIM(CAST(cus.legal_id[0] AS VARCHAR)) = ''
        THEN 'Y'
        ELSE 'N'
    END                                                                                      AS has_no_legal_id,

    -- is_legal_doc_expired: LEGAL.EXP.DATE absent from bronze -> cannot evaluate; honest NULL cast to VARCHAR
    -- (rpt_aml05 c.is_legal_doc_expired; mirrors stg_aml05__customers honest-gap handling)
    CAST(NULL AS VARCHAR)                                                                    AS is_legal_doc_expired,

    -- requires_legal_doc_update: driver condition restatement — rows in this model already satisfy
    -- the blank-legal-id arm; expired-date arm is honest NULL gap (no exp-date in bronze).
    -- Flag = 'Y' for all rows emitted (they passed the WHERE filter below); 'N' never emitted.
    -- (rpt_aml05 c.requires_legal_doc_update used as is_reportable flag)
    CAST('Y' AS VARCHAR)                                                                     AS requires_legal_doc_update,

    -- occupation: t24_customer.occupation LIST -> [0] (rpt_aml05 c.occupation)
    -- Tables.md: CUSTOMER in scope (high confidence). occupation[0] used in sibling t24_cris_report.
    TRIM(CAST(cus.occupation[0] AS VARCHAR))                                                 AS occupation,

    -- gender: t24_customer.gender (rpt_aml05 c.gender)
    -- Tables.md: CUSTOMER in scope. gender used in sibling t24_acct_cust (acc.gender) and customer.
    TRIM(CAST(cus.gender AS VARCHAR))                                                        AS gender,

    -- customer_type: CUSTOMER.TYPE — not present in bronze t24_customer (not seen in any sibling);
    -- absent-from-bronze -> honest NULL (rpt_aml05 c.customer_type)
    CAST(NULL AS VARCHAR)                                                                    AS customer_type,

    -- customer_status: CUSTOMER.STATUS — not present in bronze t24_customer (not seen in any sibling);
    -- absent-from-bronze -> honest NULL (rpt_aml05 c.customer_status)
    CAST(NULL AS VARCHAR)                                                                    AS customer_status,

    -- record_timestamp: no timestamp column on hive.bronze.t24_customer AT SNAPSHOT '{{ var("snap_customer") }}' or t24_account in scope;
    -- absent-from-bronze -> honest NULL (rpt_aml05 c.record_timestamp)
    CAST(NULL AS TIMESTAMP)                                                                  AS record_timestamp,

    -- ══════════════════════════════════════════════════════════════════
    -- Coverage additions — rpt_aml05 column targets (stg_aml05__accounts aliases)
    -- ══════════════════════════════════════════════════════════════════

    -- mis_date: COB business_date D (was CURRENT_DATE; mis_date must be COB day, not wall clock)
    date '{{ var("business_date") }}'                                                        AS mis_date,

    -- company_code: alias of comp_id (rpt_aml05 a.company_code used as branch_code/branch_name)
    TRIM(CAST(acc.co_code AS VARCHAR))                                                       AS company_code,

    -- account_id: alias of account_number (rpt_aml05 a.account_id)
    TRIM(CAST(acc.recid AS VARCHAR))                                                         AS account_id,

    -- account_title_1: ACCOUNT.TITLE.1 — not present in bronze t24_account (not seen in any sibling);
    -- absent-from-bronze -> honest NULL (rpt_aml05 a.account_title_1 as account_name)
    CAST(NULL AS VARCHAR)                                                                    AS account_title_1,

    -- account_title_2: ACCOUNT.TITLE.2 — not present in bronze t24_account (not seen in any sibling);
    -- absent-from-bronze -> honest NULL (rpt_aml05 a.account_title_2)
    CAST(NULL AS VARCHAR)                                                                    AS account_title_2,

    -- opening_date: alias of open_account_date (rpt_aml05 a.opening_date as account_opening_date)
    CAST(acc.opening_date AS DATE)                                                           AS opening_date,

    -- account_category: t24_account.category (rpt_aml05 a.account_category)
    -- Tables.md: ACCOUNT in scope (high confidence). category used in WHERE filter above.
    TRIM(CAST(acc.category AS VARCHAR))                                                      AS account_category,

    -- account_currency: t24_account.currency (rpt_aml05 a.account_currency)
    -- Tables.md: ACCOUNT in scope. currency used in sibling t24_inactive_account_report.
    TRIM(CAST(acc.currency AS VARCHAR))                                                      AS account_currency,

    -- working_balance: t24_account.working_balance (rpt_aml05 a.working_balance as account_balance)
    -- Tables.md: ACCOUNT in scope. working_balance used in sibling t24_acct_cust, t24_cris_report.
    CAST(acc.working_balance AS DECIMAL(18,2))                                               AS working_balance,

    -- dir0: partition column (MIS_DATE string) — YYYYMMDD VARCHAR from business_date D (was CURRENT_DATE)
    REPLACE(CAST(date '{{ var("business_date") }}' AS VARCHAR), '-', '')                     AS dir0,

    -- cust_account_ref: CUSTOMER.ACCOUNT.ACCOUNT_NUMBER — the account reference held on the
    -- CUSTOMER.ACCOUNT link record for this customer.
    -- logic.md §Part1/step10: "Read CUSTOMER.ACCOUNT for the customer to get the list of account ids".
    -- tables.md: CUSTOMER.ACCOUNT role=access, confidence=high.
    -- Key: cust_account_dedup.recid = cus.recid (customer id).
    -- NULL-honest: customers with no CUSTOMER.ACCOUNT record will have NULL here.
    ca.account_number                                                                        AS cust_account_ref,

    -- business_date: COB date stamp (T-STAMP; uniform across all 4 silver models)
    CAST(date '{{ var("business_date") }}' AS DATE)                AS business_date
FROM customer_dedup cus
JOIN hive.bronze.t24_account AT SNAPSHOT '{{ var("snap_account") }}' acc
    ON TRIM(CAST(acc.customer AS VARCHAR)) = TRIM(CAST(cus.recid AS VARCHAR))
LEFT JOIN company_dedup comp
    ON TRIM(CAST(comp.recid AS VARCHAR)) = TRIM(CAST(acc.co_code AS VARCHAR))
-- §14 id-doc-type description: EB.LOOKUP keyed 'CUS.LEGAL.DOC.NAME*'+legal_doc_name[0] (wired 2026-06-19)
-- Dedup: bronze t24_eb_lookup carries multiple versioned rows per recid (curr_no); keep latest so
-- this lookup stays 1:1 and does NOT fan out the driver grain (reconciled 2026-06-22: 25 dup recids, ×3).
LEFT JOIN (
    SELECT recid, lu_desc FROM (
        SELECT recid, description[0] AS lu_desc,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR)) ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_eb_lookup WHERE recid LIKE 'CUS.LEGAL.DOC.NAME%' AND business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
) luk1
    ON TRIM(CAST(luk1.recid AS VARCHAR)) = CONCAT('CUS.LEGAL.DOC.NAME*', TRIM(CAST(cus.legal_doc_name[0] AS VARCHAR)))
-- §14 GIC.ID: fallback doc-type description when EB.LOOKUP miss (wired per logic.md §7)
-- Dedup: t24_gic_id also versioned per recid (curr_no); keep latest (reconciled 2026-06-22: 15 dup recids, ×3).
LEFT JOIN (
    SELECT recid, gic_desc FROM (
        SELECT recid, description[0] AS gic_desc,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR)) ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_gic_id WHERE business_date = date '{{ var("business_date") }}'
    ) t WHERE rn = 1
) g
    ON TRIM(CAST(g.recid AS VARCHAR)) = TRIM(CAST(cus.legal_doc_name[0] AS VARCHAR))
-- §Part1/step10 CUSTOMER.ACCOUNT link: deduped 1 row per customer; no fan-out on the existing acc JOIN.
LEFT JOIN cust_account_dedup ca
    ON ca.recid = TRIM(CAST(cus.recid AS VARCHAR))
WHERE
    -- Account-level category filter (hard-coded in routine; read directly off ACCOUNT, no CATEGORY join)
    CAST(CAST(acc.category AS BIGINT) AS VARCHAR) IN
        ('6005', '6006', '6007', '6010', '1001', '1002', '1003')
    -- Post-driver qualifying condition: canonical is LEGAL.ID EQ '' OR (LEGAL.EXP.DATE LE TODAY AND NE '').
    -- FIXED 2026-06-22: legal_exp_date confirmed as ARRAY column in bronze t24_customer (reconcile).
    -- Both arms now active: blank-legal-id arm (32 rows) + expired-doc arm (1 row in current snapshot).
    AND (
        cus.legal_id IS NULL
        OR cus.legal_id[0] IS NULL
        OR TRIM(CAST(cus.legal_id[0] AS VARCHAR)) = ''
        OR (
            cus.legal_exp_date IS NOT NULL
            AND cus.legal_exp_date[0] IS NOT NULL
            AND TRIM(CAST(cus.legal_exp_date[0] AS VARCHAR)) <> ''
            AND CAST(cus.legal_exp_date[0] AS DATE) <= date '{{ var("business_date") }}'  -- was CURRENT_DATE
        )
    )
