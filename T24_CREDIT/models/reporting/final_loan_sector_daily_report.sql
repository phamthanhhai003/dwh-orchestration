{{ config(
    materialized='incremental',
    unique_key=['load_date', 'credit_by_sector', 'type_period'],
    incremental_strategy='merge',
    database='hive',
    schema='gold'
) }}

-- Division: credit | Report: loan_sector_daily
-- Available date (reconcile 2026-06-23): run with target_date=2026-06-15 (max last_date_cr in t24_cris_report; max opening_date=2026-06-11).
--   load_date is a STAMP/LABEL of target_date applied to every USD-loan cris row (NOT a data filter) -> all USD-loan rows flow.
-- Sheet: Loan Classification by Sector Daily
-- Grain: 1 row per (load_date, credit_by_sector, type_period)
-- type_period:
--   CURR = actual data for this load_date
--   PREV = previous-month snapshot copied with load_date = target_date (for side-by-side compare)
--   YOY  = same-month-last-year snapshot copied with load_date = target_date
-- Sector 7 does not exist by design.
-- Column naming: underscores used (hyphens unsupported in Dremio MERGE column names)

-- Phase 5b direct-read: ref('stg__loan_sector_detail_t24') replaced with ref('t24_cris_report').
-- Adapter shaping folded inline: currency=USD filter, ABS(loan_outstanding), COALESCE(credit_by_sector,10),
-- auto_class→asset_class, load_date derived from var("target_date").
WITH silver AS (
    SELECT
        -- load_date = target_date parameter (passed at run). NOT CURRENT_DATE.
        -- Blank target_date -> NULL load_date -> empty report (target_date is required for this report).
        CAST(CAST(NULLIF('{{ var("target_date", "") }}', '') AS DATE) AS VARCHAR) AS load_date,
        COALESCE(credit_by_sector, 10)                                             AS credit_by_sector,
        TRIM(CAST(auto_class AS VARCHAR))                                           AS asset_class,
        ABS(CAST(loan_outstanding AS DECIMAL(18,2)))                               AS balance
    FROM hive.silver.t24_cris_report
    WHERE currency = 'USD'
      AND ABS(CAST(loan_outstanding AS DECIMAL(18,2))) > 0
),

base AS (
    SELECT
        load_date,
        credit_by_sector,
        asset_class,
        balance
    FROM silver
    {% if var("target_date") != "" %}
    WHERE load_date = '{{ var("target_date") }}'
    {% elif is_incremental() %}
    WHERE load_date > (
        SELECT COALESCE(MAX(load_date), '1900-01-01')
        FROM {{ this }}
        WHERE type_period = 'CURR'
    )
    {% endif %}
),

sector_labels AS (
    SELECT 1  AS credit_by_sector, 'Agriculture, Water & Forestry'          AS naran_sector UNION ALL
    SELECT 2,  'Industry' UNION ALL
    SELECT 3,  'Manufacturing' UNION ALL
    SELECT 4,  'Construction' UNION ALL
    SELECT 5,  'Transport & Communication' UNION ALL
    SELECT 6,  'Trade & Finance' UNION ALL
    SELECT 8,  'Services' UNION ALL
    SELECT 9,  'Individuals' UNION ALL
    SELECT 10, 'Others (Other sectors not in the list)' UNION ALL
    SELECT 11, 'Tourism'
),

pivoted AS (
    SELECT
        load_date,
        credit_by_sector,
        CAST(SUM(CASE WHEN asset_class = 'STANDARD'          THEN balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS standard,
        CAST(SUM(CASE WHEN asset_class = 'UNDER-SUPERVISION' THEN balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS under_supervision,
        CAST(SUM(CASE WHEN asset_class = 'SUB-STANDARD'      THEN balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS sub_standard,
        CAST(SUM(CASE WHEN asset_class = 'DOUBTFUL'          THEN balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS doubtful,
        CAST(SUM(CASE WHEN asset_class = 'LOSS'              THEN balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS loss
    FROM base
    GROUP BY 1, 2
),

report_dates AS (
    SELECT DISTINCT load_date FROM pivoted
),

sector_grid AS (
    SELECT d.load_date, s.credit_by_sector, s.naran_sector
    FROM report_dates d
    CROSS JOIN sector_labels s
),

sector_rows AS (
    SELECT
        g.load_date,
        g.credit_by_sector,
        g.naran_sector,
        CAST(COALESCE(p.standard,          CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS standard,
        CAST(COALESCE(p.under_supervision, CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS under_supervision,
        CAST(COALESCE(p.sub_standard,      CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS sub_standard,
        CAST(COALESCE(p.doubtful,          CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS doubtful,
        CAST(COALESCE(p.loss,              CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS loss
    FROM sector_grid g
    LEFT JOIN pivoted p
        ON  g.load_date        = p.load_date
        AND g.credit_by_sector = p.credit_by_sector
),

grand_total AS (
    SELECT
        load_date,
        0                                              AS credit_by_sector,
        'Grand Total'                                  AS naran_sector,
        CAST(SUM(standard)          AS DECIMAL(18,2))  AS standard,
        CAST(SUM(under_supervision) AS DECIMAL(18,2))  AS under_supervision,
        CAST(SUM(sub_standard)      AS DECIMAL(18,2))  AS sub_standard,
        CAST(SUM(doubtful)          AS DECIMAL(18,2))  AS doubtful,
        CAST(SUM(loss)              AS DECIMAL(18,2))  AS loss
    FROM sector_rows
    GROUP BY load_date
),

combined AS (
    SELECT load_date, credit_by_sector, naran_sector, standard, under_supervision, sub_standard, doubtful, loss FROM sector_rows
    UNION ALL
    SELECT load_date, credit_by_sector, naran_sector, standard, under_supervision, sub_standard, doubtful, loss FROM grand_total
),

curr AS (
    SELECT
        load_date,
        credit_by_sector,
        naran_sector,
        standard,
        under_supervision,
        sub_standard,
        doubtful,
        loss,
        'CURR' AS type_period,
        CONCAT(
            CAST(CAST(SUBSTR(load_date, 9, 2) AS INTEGER) AS VARCHAR),
            CASE
                WHEN CAST(SUBSTR(load_date, 9, 2) AS INTEGER) IN (11, 12, 13) THEN 'th'
                WHEN MOD(CAST(SUBSTR(load_date, 9, 2) AS INTEGER), 10) = 1 THEN 'st'
                WHEN MOD(CAST(SUBSTR(load_date, 9, 2) AS INTEGER), 10) = 2 THEN 'nd'
                WHEN MOD(CAST(SUBSTR(load_date, 9, 2) AS INTEGER), 10) = 3 THEN 'rd'
                ELSE 'th'
            END,
            ' ',
            CASE CAST(SUBSTR(load_date, 6, 2) AS INTEGER)
                WHEN 1  THEN 'January'    WHEN 2  THEN 'February'   WHEN 3  THEN 'March'
                WHEN 4  THEN 'April'      WHEN 5  THEN 'May'         WHEN 6  THEN 'June'
                WHEN 7  THEN 'July'       WHEN 8  THEN 'August'      WHEN 9  THEN 'September'
                WHEN 10 THEN 'October'    WHEN 11 THEN 'November'    WHEN 12 THEN 'December'
            END,
            ' ',
            SUBSTR(load_date, 1, 4)
        ) AS load_date_label,
        CURRENT_TIMESTAMP AS updated_at
    FROM combined
),

-- PREV: previous-month snapshot — real data when incremental, empty otherwise
prev_period AS (
{% if is_incremental() and var("target_date", "") != "" %}
    SELECT
        CAST('{{ var("target_date") }}' AS VARCHAR)    AS load_date,
        credit_by_sector, naran_sector,
        standard, under_supervision, sub_standard, doubtful, loss,
        'PREV'                                         AS type_period,
        load_date_label,
        CURRENT_TIMESTAMP                              AS updated_at
    FROM {{ this }}
    WHERE type_period = 'CURR'
      AND load_date = (
          SELECT MAX(load_date) FROM {{ this }}
          WHERE  type_period = 'CURR'
            AND  CAST(load_date AS DATE) < CAST(DATE_TRUNC('month', CAST('{{ var("target_date") }}' AS DATE)) AS DATE)
      )
{% else %}
    SELECT * FROM curr WHERE 1 = 0
{% endif %}
),

-- YOY: same-month-last-year snapshot — real data when incremental, empty otherwise
yoy_period AS (
{% if is_incremental() and var("target_date", "") != "" %}
    SELECT
        CAST('{{ var("target_date") }}' AS VARCHAR)    AS load_date,
        credit_by_sector, naran_sector,
        standard, under_supervision, sub_standard, doubtful, loss,
        'YOY'                                          AS type_period,
        load_date_label,
        CURRENT_TIMESTAMP                              AS updated_at
    FROM {{ this }}
    WHERE type_period = 'CURR'
      AND load_date = (
          SELECT MAX(load_date) FROM {{ this }}
          WHERE  type_period = 'CURR'
            AND  CAST(DATE_TRUNC('year', CAST(load_date AS DATE)) AS DATE) = CAST(DATE_TRUNC('year', TIMESTAMPADD(YEAR, -1, CAST('{{ var("target_date") }}' AS DATE))) AS DATE)
            AND  CAST(load_date AS DATE) <= CAST(TIMESTAMPADD(YEAR, -1, DATE_TRUNC('month', CAST('{{ var("target_date") }}' AS DATE))) AS DATE)
      )
{% else %}
    SELECT * FROM curr WHERE 1 = 0
{% endif %}
)

SELECT * FROM curr
UNION ALL
SELECT * FROM prev_period
UNION ALL
SELECT * FROM yoy_period

ORDER BY load_date, credit_by_sector, type_period
