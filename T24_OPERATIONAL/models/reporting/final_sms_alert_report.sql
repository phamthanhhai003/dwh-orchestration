{{ config(
    materialized='incremental',
    unique_key=['load_date', 'account_id'],
    incremental_strategy='merge',
    database='hive',
    schema='gold'
) }}

-- Division: operational | Report: SMS Alert Subscribers List
-- Grain: 1 row per account at load_date
-- Template: "Report SMS Alert ba Jits.xlsx" — Sheet1
-- Columns (match template exactly):
--   Nu | Name | Account Number | Customer Number | Registered Date | Email | Contact Number | OBS
-- Note: Email     ← CUSTOMER.EMAIL.1 (sparse 3.2% in bronze t24_customer; real where present)
-- Note: Contact Number ← CUSTOMER.SMS.1 (72.7% in bronze; the SMS-alert registration number)
-- Note: OBS — *** NO MATCHING T24 FIELD *** (flagged 2026-06-24). ss_full.json search of
--   CUSTOMER + ACCOUNT found no field equal to an OBS/observations column. Nearest concepts
--   (CUSTOMER.MOBILE.BANKING.SERVICE / PREF.CHANNEL, ACCOUNT.MV.ALERT.RES*) are service/flag
--   fields, not an "SMS Alert" observation. Left BLANK (NULL) pending UAT confirmation with
--   the client on the intended source/meaning of the template OBS column.
-- Source: hive.silver.t24_acct_cust (ENQ ACCT.CUST) — read DIRECTLY, no staging layer.
--   Reads silver like the other 4 ACCT.CUST operational reports. dim_branch (municipal) +
--   template column shaping done inline here.

{% set td = var('target_date', '') %}
{% if td != '' %}{% set load_dt = td %}{% else %}{% set load_dt = modules.datetime.datetime.now().strftime('%Y-%m-%d') %}{% endif %}

WITH dim_branch AS (
    SELECT co_code, municipal
    FROM (VALUES
        ('TL0010002', 'Dili'),
        ('TL0010003', 'Ermera'),
        ('TL0010004', 'Bobonaro'),
        ('TL0010005', 'Manatuto'),
        ('TL0010006', 'Aileu'),
        ('TL0010007', 'Oecusse'),
        ('TL0010008', 'Baucau'),
        ('TL0010009', 'Manufahi'),
        ('TL0010010', 'Ainaro'),
        ('TL0010011', 'Cova Lima'),
        ('TL0010012', 'Viqueque'),
        ('TL0010013', 'Lauten'),
        ('TL0010014', 'Liquica')
    ) AS t(co_code, municipal)
),

base AS (
    SELECT
        TRIM(CAST(s.account_number AS VARCHAR))                  AS account_id,
        TRIM(CAST(s.customer_no    AS VARCHAR))                  AS customer_number,
        COALESCE(
            NULLIF(TRIM(CAST(s.name1 AS VARCHAR)), ''),
            NULLIF(TRIM(CAST(s.short_name AS VARCHAR)), '')
        )                                                        AS customer_name,
        TRIM(CAST(s.company AS VARCHAR))                         AS branch_code,
        COALESCE(b.municipal, TRIM(CAST(s.company AS VARCHAR)))  AS municipal,
        CAST(s.category AS VARCHAR)                              AS product_code,
        CASE WHEN s.opening_date IS NULL THEN NULL
             ELSE CAST(s.opening_date AS VARCHAR) END            AS registered_date,
        NULLIF(TRIM(CAST(s.email AS VARCHAR)), '')               AS email,
        NULLIF(TRIM(CAST(s.sms   AS VARCHAR)), '')               AS contact_number
    FROM hive.silver.t24_acct_cust s
    LEFT JOIN dim_branch b ON TRIM(CAST(s.company AS VARCHAR)) = b.co_code
    WHERE s.account_number IS NOT NULL
      AND TRIM(CAST(s.account_number AS VARCHAR)) NOT IN ('', 'null')
),

numbered AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY customer_name ASC, account_id ASC
        )                                                       AS nu,
        account_id,
        customer_number,
        customer_name,
        branch_code,
        municipal,
        product_code,
        registered_date,
        email,
        contact_number
    FROM base
)

SELECT
    CAST(TO_DATE('{{ load_dt }}', 'YYYY-MM-DD') AS VARCHAR)    AS load_date,
    CAST(nu AS INTEGER)                                        AS nu,
    customer_name                                              AS name,
    account_id                                                 AS account_number,
    customer_number,
    registered_date,
    email,
    contact_number,
    CAST(NULL AS VARCHAR)                                      AS obs,  -- NO MATCHING T24 FIELD — blank pending UAT
    -- Extra context cols (not in template — useful for filtering downstream)
    branch_code,
    municipal,
    product_code,
    CURRENT_TIMESTAMP                                          AS updated_at
FROM numbered
ORDER BY nu
