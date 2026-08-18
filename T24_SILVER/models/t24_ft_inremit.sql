{{ config(
    materialized='table',
    database='hive',
    schema='silver'
) }}

-- Division: treasury (authored under CREDIT_REPORTS staging) | Report: BNCTL.FT.INREMIT (t24-built source)
-- Reads real hive.bronze.t24_* per Part-1 §4 BNCTL.FT.INREMIT (T24_REPORT_DEVELOPMENT_SOT.md).
--
-- Grain: one row per FUNDS.TRANSFER record where TRANSACTION.TYPE = 'IT' (inward remittance).
--   Record key = t24_funds_transfer.recid -> transaction_ref (Field.1, Column.1).
--
-- Canonical tables (tables.md): FUNDS.TRANSFER (core) + ACCOUNT (enrichment only).
-- Join / LINK chain (§4, one hop from FUNDS.TRANSFER):
--   1. debit_acct_no  -> t24_account.recid -> short_title  ("Received From"; L ACCOUNT,SHORT.TITLE)
--   2. credit_acct_no -> t24_account.recid -> short_title  ("Beneficiary"; IDESC CREDIT.ACCT.NAME)
--   NOTE: CUSTOMER and CURRENCY are NOT in BNCTL.FT.INREMIT tables.md -> joins removed (audit 2026-06-19).
--   All enrichment joins are LEFT (an inward transfer may credit an external beneficiary acct).
--
-- Bronze->Silver conventions applied:
--   * LIST/multi-value bronze columns -> element access col[0] (NOT "col/0").
--       t24_account.short_title is LIST -> short_title[0] extracted ONCE inside the dedup CTE,
--       so the outer query references a plain VARCHAR (no double [0]).
--   * t24_account carries >1 versioned row per recid -> dedup CTE keeps latest
--       (ROW_NUMBER PARTITION BY recid ORDER BY curr_no DESC) so the 1:1 enrichment join
--       cannot fan out.
--   * Bronze date columns are real DATE -> CAST(x AS DATE). Never DATE_FORMAT.
--   * Several enquiry fields (DEBIT.THEIR.REF, packed AMOUNT.* /
--       TOTAL.CHARGE.AMT, L.FT.NO) are NOT present on bronze
--       t24_funds_transfer -> emitted as CAST(NULL AS <type>) to preserve schema coverage.
--       Amounts/currencies are sourced from the real bronze scalar columns
--       (debit_amount/debit_currency, credit_amount/credit_currency) instead of packed fields.
--   * ordering_cust (ORDERING.CUST), ord_cust_acct (ORD.CUST.ACCT), inw_send_bic (INW.SEND.BIC),
--       value_date (CREDIT.VALUE.DATE), debit_acct_no, credit_acct_no removed (audit 2026-06-19):
--       these are optional filter/join-key fields only — NOT output columns in logic.md Part-1 §4
--       or Part-2 SELECT. Present in tables.md notes as FT fields, but not in the canonical output.
--   * 'IT' fixed selection (§4) is the production-faithful active filter; gated behind
--       var('require_current') (default true) so dev column-coverage can drop it.

-- customer_dedup and currency_dedup removed: CUSTOMER and CURRENCY are NOT in BNCTL.FT.INREMIT tables.md.
-- ordering_cust / ord_cust_acct / inw_send_bic / credit_value_date / credit_acct_no / processing_date
--   are FUNDS.TRANSFER fields confirmed in tables.md notes; added for current-source schema coverage
--   (coverage pass 2026-06-20). They are optional filter inputs in logic.md §4 and real bronze columns.
WITH account_dedup AS (
    SELECT recid, short_title FROM (
        SELECT
            recid,
            short_title[0] AS short_title,
            ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR))
                               ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_account AT SNAPSHOT '{{ var("snap_account") }}'
    ) t WHERE rn = 1
)

SELECT
    -- pipeline row number: no T24 equivalent; CAST NULL for schema coverage (current source: NO)
    CAST(NULL AS INTEGER)                                                        AS row_no,
    -- Field.1 / Column.1: record key (current source: TRANSACTION_REF)
    TRIM(CAST(ft.recid AS VARCHAR))                                              AS transaction_ref,
    -- Field.2 (DEBIT.THEIR.REF) / Column.2 -- source: ft.debit_their_ref (current source: REF_NUMBER)
    TRIM(CAST(ft.debit_their_ref AS VARCHAR))                                    AS ref_number,
    -- ORDERING.CUST (tables.md FUNDS.TRANSFER field; LIST col -> [0]) -- current source: ORDERING_CUSTOMER
    TRIM(CAST(ft.ordering_cust[0] AS VARCHAR))                                   AS ordering_customer,
    -- ORD.CUST.ACCT (tables.md FUNDS.TRANSFER field) -- current source: ORDERING_CUSTOMER_ACC_NO
    TRIM(CAST(ft.ord_cust_acct AS VARCHAR))                                      AS ordering_customer_acc_no,
    -- INW.SEND.BIC (tables.md FUNDS.TRANSFER field) -- current source: SENDER_BIC
    TRIM(CAST(ft.inw_send_bic AS VARCHAR))                                       AS sender_bic,
    -- ORIGIN_OF_COUNTRY: not in logic.md canonical output or tables.md -- CAST NULL
    CAST(NULL AS VARCHAR)                                                        AS origin_of_country,
    -- DEBIT.ACCT.NO (join key for received_from) -- current source: DEBIT_ACCOUNT (the account number)
    TRIM(CAST(ft.debit_acct_no AS VARCHAR))                                      AS debit_account,
    -- Field.3 (L ACCOUNT,SHORT.TITLE on debit_acct_no) / Column.3 -- "Received From"
    TRIM(CAST(dbt.short_title AS VARCHAR))                                       AS received_from,
    -- Field.4 (IDESC CREDIT.ACCT.NAME -> ACCOUNT.SHORT.TITLE on credit_acct_no) / Column.4 -- "Beneficiary"
    TRIM(CAST(ben.short_title AS VARCHAR))                                       AS beneficiary,
    -- GENDER: not in T24 or logic.md -- CAST NULL (stg_remittance_daily also emits NULL)
    CAST(NULL AS VARCHAR)                                                        AS gender,
    -- CREDIT.ACCT.NO (FUNDS.TRANSFER field; join key for beneficiary) -- current source: CREDIT_ACCOUNT
    TRIM(CAST(ft.credit_acct_no AS VARCHAR))                                     AS credit_account,
    -- DISP.DRAMT: IF DEBIT.AMOUNT > 0 THEN DEBIT.AMOUNT ELSE CREDIT.AMOUNT (Part-1 §4)
    CASE WHEN CAST(ft.debit_amount AS DOUBLE) > 0
         THEN CAST(ft.debit_amount AS DOUBLE)
         ELSE CAST(ft.credit_amount AS DOUBLE)
    END                                                                          AS amount,
    -- Field.17 (AMOUNT.DEBITED EXTRACT 1,3) / Column.11 -- bronze: debit_currency
    TRIM(CAST(ft.debit_currency AS VARCHAR))                                     AS debit_ccy,
    -- Field.18 (AMOUNT.DEBITED EXTRACT 4,20) / Column.12 -- bronze: debit_amount
    CAST(ft.debit_amount AS DOUBLE)                                              AS debit_amount,
    -- Field.19 (TOTAL.CHARGE.AMT EXTRACT 1,3) / Column.13 -- source: ft.total_charge_amt packed CCY+amount (logic.md)
    SUBSTR(TRIM(CAST(ft.total_charge_amt AS VARCHAR)), 1, 3)                     AS charge_ccy,
    -- Field.20 (TOTAL.CHARGE.AMT EXTRACT 4,20) / Column.14 -- source: ft.total_charge_amt packed CCY+amount (logic.md)
    CAST(NULLIF(SUBSTR(TRIM(CAST(ft.total_charge_amt AS VARCHAR)), 4, 20), '') AS DOUBLE)  AS charges,
    -- Field.21 (AMOUNT.CREDITED EXTRACT 1,3) / Column.15 -- bronze: credit_currency
    TRIM(CAST(ft.credit_currency AS VARCHAR))                                    AS credit_ccy,
    -- Field.22 (AMOUNT.CREDITED EXTRACT 4,20) / Column.16 -- source: ft.amount_credited packed CCY+amount; bronze credit_amount is NULL (logic.md)
    CAST(NULLIF(SUBSTR(TRIM(CAST(ft.amount_credited AS VARCHAR)), 4, 20), '') AS DOUBLE)   AS credit_amount,
    -- CREDIT.VALUE.DATE (FUNDS.TRANSFER optional filter field, DATE) -- current source: VALUE_DATE
    CAST(ft.credit_value_date AS DATE)                                           AS value_date,
    -- DATE_TRANSFER: report date of transfer -- mapped to processing_date (DATE, FUNDS.TRANSFER)
    CAST(ft.processing_date AS DATE)                                             AS date_transfer,
    -- PROFIT.CENTRE.DEPT (Part-1 §4; Part-2 SQL profit_centre) -- real bronze column profit_centre_dept
    TRIM(CAST(ft.profit_centre_dept AS VARCHAR))                                 AS profit_centre,
    -- RATE_BNCTL: not in logic.md canonical output -- CAST NULL
    CAST(NULL AS DOUBLE)                                                         AS rate_bnctl,
    -- REMARK: not in logic.md canonical output -- CAST NULL
    CAST(NULL AS VARCHAR)                                                        AS remark,
    -- REMITTANCE_INFORMATION: not in logic.md canonical output -- CAST NULL
    CAST(NULL AS VARCHAR)                                                        AS remittance_information,
    -- Field.23 (L.FT.NO IDESC) / Column.17 -- absent in bronze (multi-value I-descriptor)
    CAST(NULL AS VARCHAR)                                                        AS ft_link,
    CAST(date '{{ var("business_date") }}' AS DATE)                AS business_date
    -- payment_details removed: not in canonical Part-1/Part-2 or tables.md (audit 2026-06-19).
    -- auth_date removed: not a canonical output field in BNCTL.FT.INREMIT (audit 2026-06-19).
    -- debit_ccy_numeric removed: CURRENCY table not in BNCTL.FT.INREMIT tables.md (audit 2026-06-19)
    -- report_date omitted: pipeline metadata from ingestion layer, not a T24/silver field.
FROM hive.bronze.t24_funds_transfer AT SNAPSHOT '{{ var("snap_funds_transfer") }}' ft
LEFT JOIN account_dedup  dbt ON TRIM(CAST(ft.debit_acct_no  AS VARCHAR)) = TRIM(CAST(dbt.recid AS VARCHAR))
LEFT JOIN account_dedup  ben ON TRIM(CAST(ft.credit_acct_no AS VARCHAR)) = TRIM(CAST(ben.recid AS VARCHAR))
-- customer_dedup and currency_dedup joins removed: not in BNCTL.FT.INREMIT tables.md (audit 2026-06-19)
-- §4 fixed mandatory selection (canonical, unconditional): inward remittances only.
-- Gated behind var('require_current', true): set to false in dev to prove column coverage when IT absent.
{% if var('require_current', true) %}
WHERE TRIM(CAST(ft.transaction_type AS VARCHAR)) = 'IT'
{% endif %}
