{{ config(
    materialized = 'table',
    database = 'hive',
    schema = 'gold',
    tags = ['remittance']
) }}

-- Direct-read from silver (stg_remittance_daily_t24 adapter removed from lineage).
-- Column renames + report_date injection folded in from the former staging adapter.
-- Output schema is byte-identical to before: same column names and order.
--
-- Column gap notes (honest NULLs from silver, unchanged):
--   "NO"                     — silver row_no is CAST(NULL AS INTEGER)
--   "ORIGIN OF COUNTRY"      — silver origin_of_country is CAST(NULL AS VARCHAR)
--   "GENDER"                 — silver gender is CAST(NULL AS VARCHAR)
--   "RATE BNCTL"             — silver rate_bnctl is CAST(NULL AS DOUBLE)
--   "REMARK"                 — silver remark is CAST(NULL AS VARCHAR)
--   "REMITTANCE INFORMATION" — silver remittance_information is CAST(NULL AS VARCHAR)
-- Fields resolved (2026-06-22):
--   "REF NUMBER"  — now from ft.debit_their_ref
--   "CHARGE CCY"  — now SUBSTR(total_charge_amt,1,3)
--   "CHARGES"     — now SUBSTR(total_charge_amt,4,20) CAST DOUBLE
--   "CREDIT AMOUNT" — now SUBSTR(amount_credited,4,20) CAST DOUBLE
-- report_date — now s.value_date (was var("target_date") literal inject)

SELECT
    s.row_no                                                        AS "NO",
    s.date_transfer                                                 AS "DATE TRANSFER",
    s.transaction_ref                                               AS "TRANSACTION REF",
    s.ref_number                                                    AS "REF NUMBER",
    s.ordering_customer                                             AS "ORDERING CUSTOMER",
    s.ordering_customer_acc_no                                      AS "ORDERING CUSTOMER ACC NO",
    s.sender_bic                                                    AS "SENDER BIC",
    s.origin_of_country                                             AS "ORIGIN OF COUNTRY",
    s.debit_account                                                 AS "DEBIT ACCOUNT",
    s.beneficiary                                                   AS "BENEFICIARY",
    s.gender                                                        AS "GENDER",
    s.credit_account                                                AS "CREDIT ACCOUNT",
    s.debit_ccy                                                     AS "DEBIT CCY",
    s.debit_amount                                                  AS "DEBIT AMOUNT",
    s.charge_ccy                                                    AS "CHARGE CCY",
    s.charges                                                       AS "CHARGES",
    s.credit_ccy                                                    AS "CREDIT CCY",
    s.credit_amount                                                 AS "CREDIT AMOUNT",
    s.rate_bnctl                                                    AS "RATE BNCTL",
    s.remark                                                        AS "REMARK",
    s.remittance_information                                        AS "REMITTANCE INFORMATION",
    s.value_date                                                   AS report_date

FROM hive.silver.t24_ft_inremit s

-- Guarded date filter (matches final_opening_account_report / final_list_of_deposits_report convention):
-- when target_date is unset, return all rows; when set, filter to that transfer date.
{% if var("target_date", "") != "" %}
WHERE s.date_transfer = CAST('{{ var("target_date") }}' AS DATE)
{% endif %}
