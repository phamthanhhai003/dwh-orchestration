{{ config(
    materialized='table',
    database='hive',
    schema='silver'
) }}

-- Division: credit | T24 object: AC.LOCKED.AMT (t24-built source)
-- Reads real hive.bronze.t24_* per Part-1 §3 AC.LOCKED.AMT (T24_REPORT_DEVELOPMENT_SOT.md).
-- "Details of Locked Amounts" / C7 Lock Obligation report: a flat list of locked-amount /
-- fund-reservation events, one row per AC.LOCKED.EVENTS record.
--
-- Grain: one row per t24_ac_locked_events record (the §3 base file / record key).
-- Join / LINK chain (§3): single hop
--   t24_ac_locked_events.account_number -> t24_account.@ID  (LEFT JOIN, fetch short_title).
-- An AC.LOCKED.EVENTS record with no matching ACCOUNT row returns NULL for Name (LEFT JOIN),
-- never dropped.
--
-- §3 selection / filter logic: the enquiry's only filter (ACCOUNT.NUMBER EQ <account_no>) is
-- optional and omitted by default (Attributes.2 = No.selection) -> returns all rows, no company
-- filter (Company Select = All). There is therefore no production "active-row" gate; the
-- require_current var still guards the (no-op) active filter block so dev coverage can drop it,
-- per bronze convention.
--
-- txn_sign (Field.9, TXN.SIGN) is a computation-only intermediate (no Column assignment); it is
-- NOT emitted as a standalone column. Only the resolved label SIGN.DISP (Field.14) is rendered as
-- reservation_type. ZERORECORDS/HEADER (Fields 15/2) are static UI literals, not columns.
--
-- LOCKED.AMOUNT (Field.8) is a plain signed numeric in bronze (Class-posneg is display-only;
-- no packed CCY+amount split). Bronze date columns (from_date, to_date) are real DATE -> CAST AS DATE.
--
-- Audit fix 2026-06-19: t24_account is a versioned dim (multiple rows per recid with curr_no).
-- Plain JOIN without dedup caused row fan-out. Fixed: account_dedup CTE keeps latest row per
-- recid using ROW_NUMBER() OVER (PARTITION BY recid ORDER BY curr_no DESC).
-- short_title is LIST/array in bronze -> short_title[0] extracted inside dedup CTE.

WITH account_dedup AS (
    SELECT recid, short_title
    FROM (
        SELECT
            recid,
            short_title[0]  AS short_title,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(CAST(recid AS VARCHAR))
                ORDER BY curr_no DESC
            ) AS rn
        FROM hive.bronze.t24_account
    ) t
    WHERE rn = 1
)

SELECT
    TRIM(CAST(ev.trans_ref AS VARCHAR))                            AS transaction_ref,
    TRIM(CAST(ev.account_number AS VARCHAR))                       AS account,
    TRIM(CAST(acc.short_title AS VARCHAR))                         AS name,
    TRIM(CAST(ev.description AS VARCHAR))                          AS reasons_for_locking,
    CAST(ev.from_date AS DATE)                                     AS from_date,
    CAST(ev.to_date AS DATE)                                       AS to_date,
    CAST(ev.locked_amount AS DECIMAL(18,2))                        AS locked_amount,
    CASE WHEN TRIM(CAST(ev.txn_sign AS VARCHAR)) = 'CR'
         THEN 'CREDIT' ELSE 'DEBIT' END                           AS reservation_type
    -- updated_at (CURRENT_TIMESTAMP) removed: no canonical backing in logic.md / tables.md
FROM hive.bronze.t24_ac_locked_events ev
LEFT JOIN account_dedup acc
       ON TRIM(CAST(ev.account_number AS VARCHAR)) = TRIM(CAST(acc.recid AS VARCHAR))
-- §3: no production active-row filter (No.selection / Company Select = All) — canonical has no selection.
