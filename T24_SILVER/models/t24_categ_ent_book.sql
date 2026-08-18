{{ config(materialized='table', database='hive', schema='silver') }}

-- §11 EM.CATEG.ENT.BOOK — GL journal entry book (silver materialization).
-- Canonical tables: CATEG.ENTRY, CATEG.ENTRY.DETAIL, CATEG.ENTRY.DETAIL.XREF, CATEG.ENT.TODAY,
--   CATEGORY, TRANSACTION, COMPANY, PL.CLOSE.DATES (tables.md).
-- CATEG.ENT.TODAY: in tables.md (core) but absent from bronze -> same-day entries unavailable.
-- PL.CLOSE.DATES: in tables.md but absent from bronze -> opening balance NOT emitted (honest NULL).
-- COMPANY: in tables.md; present in bronze; used by canonical for multi-company aggregation
--   (runtime parameter filter, no output field emitted in staging model).
-- EB.LOOKUP (PL.GAAP.TYPE), AU.COMPANY.DECOMMISSION: speculation in tables.md -> not modelled.
-- Entry universe = live CATEG.ENTRY + archived CATEG.ENTRY.DETAIL (deduped by recid;
-- xref-linked path inoperative, t24_categ_entry_detail_xref = 0 rows §5B).
-- Dim dedup: t24_category and t24_transaction are versioned dims -> curr_no DESC (fix 2026-06-19).

WITH entries AS (
    SELECT recid, pl_category, transaction_code, trans_reference, their_reference,
           value_date, booking_date, amount_lcy, amount_fcy, currency,
           account_number, company_code, customer_id,
           'LIVE' AS source
    FROM hive.bronze.t24_categ_entry
    UNION ALL
    SELECT recid, pl_category, transaction_code, trans_reference, their_reference,
           value_date, booking_date, amount_lcy, amount_fcy, currency,
           account_number, company_code, customer_id,
           'ARCHIVE' AS source
    FROM hive.bronze.t24_categ_entry_detail
),
entries_dedup AS (
    SELECT recid, pl_category, transaction_code, trans_reference, their_reference,
           value_date, booking_date, amount_lcy, amount_fcy, currency,
           account_number, company_code, customer_id, source
    FROM (
        SELECT e.*, ROW_NUMBER() OVER (
            PARTITION BY TRIM(CAST(recid AS VARCHAR))
            ORDER BY CASE WHEN source='LIVE' THEN 0 ELSE 1 END) AS rn
        FROM entries e
    ) t WHERE rn = 1
),
category_dedup AS (
    -- t24_category: versioned dim -> dedup to latest version per recid using curr_no DESC.
    -- short_name is a LIST -> [0]. Fix 2026-06-19: was erroneously ORDER BY recid DESC.
    SELECT recid, short_name FROM (
        SELECT recid, short_name[0] AS short_name,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR)) ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_category
    ) t WHERE rn = 1
),
transaction_dedup AS (
    -- t24_transaction: versioned dim -> dedup to latest version per recid using curr_no DESC.
    -- Fix 2026-06-19: was erroneously ORDER BY recid DESC (recurring bug; confirmed via sibling models).
    -- Fix 2026-06-19: short_desc -> stmt_narr (confirmed bronze column; short_desc absent from t24_transaction;
    --   sibling models t24_daily_txn_9k, t24_cui_stmt01, t24_stmt_ent_book_cui all use stmt_narr).
    SELECT recid, stmt_narr FROM (
        SELECT recid, stmt_narr,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR)) ORDER BY curr_no DESC) AS rn
        FROM hive.bronze.t24_transaction
    ) t WHERE rn = 1
),
-- §11/§5B CATEG.ENTRY.DETAIL.XREF: netted-entry -> detail pointer (wired 2026-06-20; 0 rows in bronze -> NULL).
-- Links a netted CATEG.ENTRY (recid) to its CATEG.ENTRY.DETAIL (detail_id); deduped to 1 per entry.
categ_xref_dedup AS (
    SELECT recid, detail_id FROM (
        SELECT TRIM(CAST(recid AS VARCHAR)) AS recid, TRIM(CAST(detail_id AS VARCHAR)) AS detail_id,
               ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(recid AS VARCHAR)) ORDER BY TRIM(CAST(detail_id AS VARCHAR)) DESC) AS rn
        FROM hive.bronze.t24_categ_entry_detail_xref
    ) t WHERE rn = 1
)

SELECT
    TRIM(CAST(e.recid AS VARCHAR))                                  AS entry_id,
    e.source                                                       AS source,
    CAST(e.pl_category AS INTEGER)                                  AS category,
    TRIM(CAST(cat.short_name AS VARCHAR))                           AS category_desc,
    TRIM(CAST(e.transaction_code AS VARCHAR))                       AS transaction_code,
    TRIM(CAST(trn.stmt_narr AS VARCHAR))                            AS description,
    TRIM(CAST(e.trans_reference AS VARCHAR))                        AS trans_ref,
    TRIM(CAST(e.their_reference AS VARCHAR))                        AS their_reference,
    CAST(e.value_date AS DATE)                                     AS value_date,
    CAST(e.booking_date AS DATE)                                   AS post_date,
    CAST(e.amount_lcy AS DECIMAL(18,2))                            AS amount_lcy,
    CASE WHEN CAST(e.value_date AS DATE) > CURRENT_DATE THEN '*' ELSE '' END AS forward_flag,
    TRIM(CAST(e.account_number AS VARCHAR))                         AS account_number,
    TRIM(CAST(e.company_code AS VARCHAR))                           AS company_code,
    TRIM(CAST(e.customer_id AS VARCHAR))                            AS customer_id,
    -- §11/§5B CATEG.ENTRY.DETAIL.XREF detail pointer (wired 2026-06-20; 0-row bronze -> NULL)
    xr.detail_id                                                   AS detail_xref_id,
    -- Coverage fields for minio.raw."journal_mock"."journal_accounting_bd_deposit_jan25.csv"
    -- §11 "local CCY" / "currency" — CATEG.ENTRY.currency (tables.md, high confidence).
    TRIM(CAST(e.currency AS VARCHAR))                              AS currency,
    -- §11 FCY amount — CATEG.ENTRY.amount_fcy (tables.md); NULL in dev (all USD deposits).
    CAST(e.amount_fcy AS DECIMAL(18,2))                           AS amount_fcy,
    -- CSV PRODUCT NAME (deposit product label, e.g. "02.Government") — not in tables.md
    -- tables; no equivalent bronze column derivable from CATEG.ENTRY/CATEGORY/TRANSACTION.
    CAST(NULL AS VARCHAR)                                          AS product_name,
    -- CSV CUSTOMER (customer display name) — bronze carries only numeric customer_id;
    -- customer name table not in tables.md scope -> honest NULL.
    CAST(NULL AS VARCHAR)                                          AS customer_name
FROM entries_dedup e
LEFT JOIN category_dedup    cat ON CAST(CAST(e.pl_category AS INTEGER) AS VARCHAR) = TRIM(CAST(cat.recid AS VARCHAR))
LEFT JOIN transaction_dedup trn ON TRIM(CAST(e.transaction_code AS VARCHAR))       = TRIM(CAST(trn.recid AS VARCHAR))
LEFT JOIN categ_xref_dedup  xr  ON TRIM(CAST(e.recid AS VARCHAR))                  = TRIM(CAST(xr.recid AS VARCHAR))
