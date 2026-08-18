{{ config(
    materialized='table',
    database='hive',
    schema='silver'
) }}

-- Division: credit | Object: ACCT.CUST (t24-built source, Part-1 §1)
-- Reads real hive.bronze.t24_* per Part-1 §1 ACCT.CUST (T24_REPORT_DEVELOPMENT_SOT.md).
-- Source: "EM Account & Customer Table Extract" DFE.
--
-- Grain: one row per qualifying ACCOUNT record (id = account number).
-- Join / LINK chain (§1): ACCOUNT (base) → CUSTOMER via ACCOUNT.customer = CUSTOMER.@ID
--   (single hop only; no CATEGORY param join — L ACCOUNT,CATEGORY reads the code off ACCOUNT).
--
-- Selection / filter (§1): CUSTOMER != '' AND CATEGORY >= '6006' AND CATEGORY <= '6015'
--   (string GE/LE on numeric-looking codes == integer range 6006-6015 inclusive).
--   *** DEVIATES FROM CANONICAL ACCT.CUST DFE (spec gate is >= '6007'). *** Lower bound widened
--   to 6006 on 2026-06-23 to include "Futuru Savings" (Ha'u-Nia Futuru) for the HNF reports.
--   See the inline DFE-deviation note at the WHERE clause below. Revert to '6007' to restore spec.
--
-- Bronze conventions:
--   * Array/multivalue bronze columns are LIST type → element access col[0] (NOT "col/0").
--     IBAN = alt_acct_id[0] (multi-value, first IBAN).
--   * Versioned dim t24_customer carries multiple rows per recid → dedup to latest
--     version per recid (curr_no DESC; business_date dropped for customer) to keep 1:1.
--   * Bronze date columns are real DATE → CAST(x AS DATE). Never DATE_FORMAT.
--
-- Coverage additions (schema reconcile against minio.raw."master_deposits_mock"."master_deposit_raw.csv"):
--   Fields outside ACCT.CUST DFE logic.md scope are emitted as honest CAST(NULL) to ensure
--   the silver model covers every column in the current source and serves all downstream
--   legacy stg model column targets (stg__list_of_deposits, stg__opening_account,
--   stg__hnf_account_detail, stg__sms_alert_subscribers).
--   Honest-NULL fields: currency, interest_rate, interest_amount, value_date, maturity_date,
--   branch_name, product_name, nationality, birth_country, address, occupation,
--   legal_doc_name, legal_id.

WITH customer_dedup AS (
    SELECT
        recid,
        birth_incorp_date,
        date_of_birth,
        name_1,
        name_2,
        short_name,
        nationality,
        cust_birth_country,
        street,
        address,
        town_country,
        occupation,
        legal_doc_name,
        legal_id,
        email,
        sms,
        phone
    FROM (
        SELECT
            recid,
            birth_incorp_date,
            date_of_birth,
            name_1[0]          AS name_1,
            name_2[0]          AS name_2,
            short_name[0]      AS short_name,
            nationality,
            cust_birth_country,
            street[0]          AS street,
            address[0]         AS address,
            town_country[0]    AS town_country,
            occupation[0]      AS occupation,
            legal_doc_name[0]  AS legal_doc_name,
            legal_id[0]        AS legal_id,
            email_1[0]         AS email,
            sms_1[0]           AS sms,
            phone_1[0]         AS phone,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(CAST(recid AS VARCHAR))
                ORDER BY curr_no DESC
            ) AS rn
        FROM hive.bronze.t24_customer AT SNAPSHOT '{{ var("snap_customer") }}'
    ) t
    WHERE rn = 1
)

SELECT
    -- 1 (id) AccountNumber — t24_account.@ID (record key, id position 1)
    TRIM(CAST(acc.recid AS VARCHAR))                                AS account_number,
    -- 2 Category — t24_account.category (L ACCOUNT,CATEGORY; code read off ACCOUNT)
    CAST(acc.category AS INTEGER)                                   AS category,
    -- 3 CustomerNo — t24_account.customer (direct, position 1)
    TRIM(CAST(acc.customer AS VARCHAR))                             AS customer_no,
    -- 4 OpeningDate — t24_account.opening_date (direct, position 78); bronze real DATE
    CAST(acc.opening_date AS DATE)                                  AS opening_date,
    -- 5 DateOfBirth — canonical DOB = COALESCE(BIRTH.INCORP.DATE, DATE.OF.BIRTH)
    --   (Registry Part1 §step6 / §6303 / §6830). Bronze cols: birth_incorp_date (c42) +
    --   date_of_birth (c64). HNF children carry DOB in date_of_birth (c64) more often than
    --   birth_incorp_date, so the COALESCE fallback is REQUIRED for HNF coverage. (x_date_of_birth
    --   is a different local-ref column, 0 for HNF — do NOT use it.) Fallback fixed 2026-06-23.
    CAST(COALESCE(cus.birth_incorp_date, cus.date_of_birth) AS DATE) AS date_of_birth,
    -- 6 Name1 — t24_customer.name_1 (L CUSTOMER,NAME.1)
    TRIM(CAST(cus.name_1 AS VARCHAR))                              AS name1,
    -- 7 Name2 — t24_customer.name_2 (L CUSTOMER,NAME.2)
    TRIM(CAST(cus.name_2 AS VARCHAR))                              AS name2,
    -- 8 ShortName — t24_customer.short_name (L CUSTOMER,SHORT.NAME)
    TRIM(CAST(cus.short_name AS VARCHAR))                          AS short_name,
    -- 9 Balance — t24_account.working_balance (direct, position 27)
    CAST(acc.working_balance AS DECIMAL(18,2))                     AS balance,
    -- 10 Company — t24_account.co_code (direct, position 207)
    TRIM(CAST(acc.co_code AS VARCHAR))                             AS company,
    -- 11 Gender — t24_account.gender (ACCOUNT position 20.3)
    TRIM(CAST(acc.gender AS VARCHAR))                              AS gender,
    -- 12 IBAN — t24_account.alt_acct_id[0] (multi-value, first element; LIST → [0])
    TRIM(CAST(acc.alt_acct_id[0] AS VARCHAR))                      AS iban,

    -- === Minio source coverage (CAST NULL: outside ACCT.CUST logic.md §1 scope) ===
    -- CURRENCY (minio col 2): not a DFE output field in logic.md §1
    CAST(NULL AS VARCHAR)                                           AS currency,
    -- INT RATE (minio col 6): not in logic.md §1; no equivalent in t24_account bronze
    CAST(NULL AS DECIMAL(18,6))                                     AS interest_rate,
    -- INT (minio col 7): not in logic.md §1; no equivalent in t24_account bronze
    CAST(NULL AS DECIMAL(18,2))                                     AS interest_amount,
    -- VALUE DATE (minio col 8): not in logic.md §1
    CAST(NULL AS DATE)                                              AS value_date,
    -- MAT DATE (minio col 9): not in logic.md §1; t24_account has no maturity_date col
    CAST(NULL AS DATE)                                              AS maturity_date,
    -- BRANCH NAME (minio col 10): logic.md §1 exposes CO.CODE (company) not branch name text
    CAST(NULL AS VARCHAR)                                           AS branch_name,
    -- PRODUCT NAME (minio col 12): not in logic.md §1; no param-table join in scope
    CAST(NULL AS VARCHAR)                                           AS product_name,

    -- === Downstream stg coverage (real values: in-scope t24_customer table, tables.md) ===
    -- stg__opening_account / stg__hnf_account_detail need: nationality, birth_country,
    -- address, occupation, legal_doc_name, legal_id.
    -- These fields exist in hive.bronze.t24_customer AT SNAPSHOT '{{ var("snap_customer") }}' (in-scope per ACCT.CUST tables.md).
    -- They are not DFE output fields per logic.md §1, but they are needed for downstream
    -- migration and the source table IS in scope — mapped via the existing customer_dedup CTE.
    TRIM(CAST(cus.nationality AS VARCHAR))                          AS nationality,
    TRIM(CAST(cus.cust_birth_country AS VARCHAR))                   AS birth_country,
    COALESCE(
        TRIM(CAST(cus.street AS VARCHAR)),
        TRIM(CAST(cus.address AS VARCHAR)),
        TRIM(CAST(cus.town_country AS VARCHAR))
    )                                                               AS address,
    TRIM(CAST(cus.occupation AS VARCHAR))                           AS occupation,
    TRIM(CAST(cus.legal_doc_name AS VARCHAR))                       AS legal_doc_name,
    TRIM(CAST(cus.legal_id AS VARCHAR))                             AS legal_id,

    -- === Contact fields (real values: in-scope t24_customer table, CUSTOMER DFE) ===
    -- Needed by stg__sms_alert_subscribers / final_sms_alert_report (Email + Contact Number).
    -- Canonical CUSTOMER fields (ss_full.json RECID=CUSTOMER): EMAIL.1, SMS.1, PHONE.1.
    -- Bronze cols are multi-value LIST → first element [0]. Mapped via existing customer_dedup CTE.
    --   email  = email_1[0]  (EMAIL.1) — sparse 3.2% pop in bronze, real where present.
    --   sms    = sms_1[0]    (SMS.1)   — 72.7% pop; the SMS-alert registration number.
    --   phone  = phone_1[0]  (PHONE.1) — 21.7% pop; secondary contact.
    TRIM(CAST(cus.email AS VARCHAR))                                AS email,
    TRIM(CAST(cus.sms AS VARCHAR))                                  AS sms,
    TRIM(CAST(cus.phone AS VARCHAR))                                AS phone,
    CAST(date '{{ var("business_date") }}' AS DATE)                AS business_date
FROM hive.bronze.t24_account AT SNAPSHOT '{{ var("snap_account") }}' acc
LEFT JOIN customer_dedup cus
    ON TRIM(CAST(acc.customer AS VARCHAR)) = TRIM(CAST(cus.recid AS VARCHAR))
WHERE acc.customer IS NOT NULL
  AND TRIM(CAST(acc.customer AS VARCHAR)) <> ''
  -- *** DFE GATE DEVIATION (2026-06-23) ***
  -- Canonical ACCT.CUST DFE spec gate is CATEGORY >= '6007' (Registry Part 6 §1 / logic.md §1).
  -- Lower bound widened 6007 -> 6006 to admit category 6006 = "Futuru Savings" (Ha'u-Nia Futuru
  -- children's savings), the product the OPERATIONAL HNF reports (final_hnf_by_age/gender) require.
  -- Without this, all 6006 accounts are excluded upstream and both HNF reports return 0 account data.
  -- DEVIATES FROM CANONICAL DFE: this admits 1,000 extra 6006 accounts into t24_acct_cust, also
  -- visible to final_opening_account_report (no category filter). final_list_of_deposits_report is
  -- unaffected (it filters explicit categories 6007/6008/6010/6011/6012). Revert to '6007' to restore spec.
  AND TRIM(CAST(acc.category AS VARCHAR)) >= '6006'
  AND TRIM(CAST(acc.category AS VARCHAR)) <= '6015'
