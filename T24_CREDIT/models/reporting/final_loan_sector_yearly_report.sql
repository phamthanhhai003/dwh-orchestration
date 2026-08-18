{% set has_td = var("target_date", "") != "" %}

{{ config(
    materialized='incremental',
    unique_key=['load_date', 'credit_by_sector', 'type_period'],
    incremental_strategy='merge',
    database='hive',
    schema='gold'
) }}

-- Division: credit | Report: loan_sector_yearly
-- Grain: 1 row per (year, sector) + Grand Total per year, pivoted by month.
-- month_dec_prior = December of the prior year; month_jan..month_nov = Jan-Nov of the report year.
-- Period selection (no CURRENT_DATE / now() default — driven purely by the target_date run param):
--   target_date passed -> that single year; load_date = target_date.
--   target_date blank   -> ALL years present in source (full data); load_date = '<year>-12-31' (one stamp per year).

WITH sector_labels AS (
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

silver AS (
    SELECT
        COALESCE(credit_by_sector, 10)                                             AS credit_by_sector,
        ABS(CAST(loan_outstanding AS DECIMAL(18,2)))                               AS balance,
        CAST(EXTRACT(YEAR  FROM CAST(opening_date AS DATE)) AS INTEGER)            AS load_year,
        CAST(EXTRACT(MONTH FROM CAST(opening_date AS DATE)) AS INTEGER)            AS load_month
    FROM hive.silver.t24_cris_report
    WHERE currency = 'USD'
      AND ABS(CAST(loan_outstanding AS DECIMAL(18,2))) > 0
      AND opening_date IS NOT NULL
),

-- Report year(s): target_date -> single year derived from it; blank -> all years present in source.
report_years AS (
    {% if has_td %}
    SELECT {{ (var("target_date") | string)[0:4] | int }} AS report_year
    {% else %}
    SELECT DISTINCT load_year AS report_year FROM silver
    {% endif %}
),

pivoted AS (
    SELECT
        ry.report_year,
        s.credit_by_sector,
        CAST(SUM(CASE WHEN s.load_year = ry.report_year - 1 AND s.load_month = 12 THEN s.balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS month_dec_prior,
        CAST(SUM(CASE WHEN s.load_year = ry.report_year     AND s.load_month = 1  THEN s.balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS month_jan,
        CAST(SUM(CASE WHEN s.load_year = ry.report_year     AND s.load_month = 2  THEN s.balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS month_feb,
        CAST(SUM(CASE WHEN s.load_year = ry.report_year     AND s.load_month = 3  THEN s.balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS month_mar,
        CAST(SUM(CASE WHEN s.load_year = ry.report_year     AND s.load_month = 4  THEN s.balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS month_apr,
        CAST(SUM(CASE WHEN s.load_year = ry.report_year     AND s.load_month = 5  THEN s.balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS month_may,
        CAST(SUM(CASE WHEN s.load_year = ry.report_year     AND s.load_month = 6  THEN s.balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS month_jun,
        CAST(SUM(CASE WHEN s.load_year = ry.report_year     AND s.load_month = 7  THEN s.balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS month_jul,
        CAST(SUM(CASE WHEN s.load_year = ry.report_year     AND s.load_month = 8  THEN s.balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS month_aug,
        CAST(SUM(CASE WHEN s.load_year = ry.report_year     AND s.load_month = 9  THEN s.balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS month_sep,
        CAST(SUM(CASE WHEN s.load_year = ry.report_year     AND s.load_month = 10 THEN s.balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS month_oct,
        CAST(SUM(CASE WHEN s.load_year = ry.report_year     AND s.load_month = 11 THEN s.balance ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(18,2)) AS month_nov
    FROM report_years ry
    JOIN silver s
        ON (s.load_year = ry.report_year     AND s.load_month BETWEEN 1 AND 11)
        OR (s.load_year = ry.report_year - 1 AND s.load_month = 12)
    GROUP BY ry.report_year, s.credit_by_sector
),

sector_rows AS (
    SELECT
        {% if has_td %}
        CAST(CAST(NULLIF('{{ var("target_date", "") }}', '') AS DATE) AS VARCHAR)            AS load_date,
        {% else %}
        CONCAT(CAST(ry.report_year AS VARCHAR), '-12-31')                                    AS load_date,
        {% endif %}
        sl.credit_by_sector,
        sl.naran_sector,
        CAST(COALESCE(p.month_dec_prior, CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS month_dec_prior,
        CAST(COALESCE(p.month_jan,       CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS month_jan,
        CAST(COALESCE(p.month_feb,       CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS month_feb,
        CAST(COALESCE(p.month_mar,       CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS month_mar,
        CAST(COALESCE(p.month_apr,       CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS month_apr,
        CAST(COALESCE(p.month_may,       CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS month_may,
        CAST(COALESCE(p.month_jun,       CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS month_jun,
        CAST(COALESCE(p.month_jul,       CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS month_jul,
        CAST(COALESCE(p.month_aug,       CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS month_aug,
        CAST(COALESCE(p.month_sep,       CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS month_sep,
        CAST(COALESCE(p.month_oct,       CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS month_oct,
        CAST(COALESCE(p.month_nov,       CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS month_nov
    FROM report_years ry
    CROSS JOIN sector_labels sl
    LEFT JOIN pivoted p
        ON p.report_year = ry.report_year
       AND p.credit_by_sector = sl.credit_by_sector
),

grand_total AS (
    SELECT
        load_date,
        0 AS credit_by_sector,
        'Grand Total' AS naran_sector,
        CAST(SUM(month_dec_prior) AS DECIMAL(18,2)) AS month_dec_prior,
        CAST(SUM(month_jan)       AS DECIMAL(18,2)) AS month_jan,
        CAST(SUM(month_feb)       AS DECIMAL(18,2)) AS month_feb,
        CAST(SUM(month_mar)       AS DECIMAL(18,2)) AS month_mar,
        CAST(SUM(month_apr)       AS DECIMAL(18,2)) AS month_apr,
        CAST(SUM(month_may)       AS DECIMAL(18,2)) AS month_may,
        CAST(SUM(month_jun)       AS DECIMAL(18,2)) AS month_jun,
        CAST(SUM(month_jul)       AS DECIMAL(18,2)) AS month_jul,
        CAST(SUM(month_aug)       AS DECIMAL(18,2)) AS month_aug,
        CAST(SUM(month_sep)       AS DECIMAL(18,2)) AS month_sep,
        CAST(SUM(month_oct)       AS DECIMAL(18,2)) AS month_oct,
        CAST(SUM(month_nov)       AS DECIMAL(18,2)) AS month_nov
    FROM sector_rows
    GROUP BY load_date
),

curr AS (
    SELECT
        load_date,
        credit_by_sector,
        naran_sector,
        month_dec_prior,
        month_jan, month_feb, month_mar, month_apr, month_may, month_jun,
        month_jul, month_aug, month_sep, month_oct, month_nov,
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
                WHEN 1  THEN 'January'   WHEN 2  THEN 'February'  WHEN 3  THEN 'March'
                WHEN 4  THEN 'April'     WHEN 5  THEN 'May'        WHEN 6  THEN 'June'
                WHEN 7  THEN 'July'      WHEN 8  THEN 'August'     WHEN 9  THEN 'September'
                WHEN 10 THEN 'October'   WHEN 11 THEN 'November'   WHEN 12 THEN 'December'
            END,
            ' ',
            SUBSTR(load_date, 1, 4)
        ) AS load_date_label,
        CURRENT_TIMESTAMP AS updated_at
    FROM sector_rows
    UNION ALL
    SELECT
        load_date, credit_by_sector, naran_sector,
        month_dec_prior,
        month_jan, month_feb, month_mar, month_apr, month_may, month_jun,
        month_jul, month_aug, month_sep, month_oct, month_nov,
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
                WHEN 1  THEN 'January'   WHEN 2  THEN 'February'  WHEN 3  THEN 'March'
                WHEN 4  THEN 'April'     WHEN 5  THEN 'May'        WHEN 6  THEN 'June'
                WHEN 7  THEN 'July'      WHEN 8  THEN 'August'     WHEN 9  THEN 'September'
                WHEN 10 THEN 'October'   WHEN 11 THEN 'November'   WHEN 12 THEN 'December'
            END,
            ' ',
            SUBSTR(load_date, 1, 4)
        ) AS load_date_label,
        CURRENT_TIMESTAMP AS updated_at
    FROM grand_total
)

{% if is_incremental() and has_td %}
,

prev_period AS (
    SELECT
        CAST('{{ var("target_date") }}' AS VARCHAR) AS load_date,
        credit_by_sector, naran_sector,
        month_dec_prior,
        month_jan, month_feb, month_mar, month_apr, month_may, month_jun,
        month_jul, month_aug, month_sep, month_oct, month_nov,
        'PREV' AS type_period,
        load_date_label,
        CURRENT_TIMESTAMP AS updated_at
    FROM {{ this }}
    WHERE type_period = 'CURR'
      AND load_date = (
          SELECT MAX(load_date) FROM {{ this }}
          WHERE type_period = 'CURR'
            AND CAST(load_date AS DATE) < CAST(DATE_TRUNC('month', CAST('{{ var("target_date") }}' AS DATE)) AS DATE)
      )
),

yoy_period AS (
    SELECT
        CAST('{{ var("target_date") }}' AS VARCHAR) AS load_date,
        credit_by_sector, naran_sector,
        month_dec_prior,
        month_jan, month_feb, month_mar, month_apr, month_may, month_jun,
        month_jul, month_aug, month_sep, month_oct, month_nov,
        'YOY' AS type_period,
        load_date_label,
        CURRENT_TIMESTAMP AS updated_at
    FROM {{ this }}
    WHERE type_period = 'CURR'
      AND load_date = (
          SELECT MAX(load_date) FROM {{ this }}
          WHERE type_period = 'CURR'
            AND CAST(DATE_TRUNC('year', CAST(load_date AS DATE)) AS DATE) = CAST(DATE_TRUNC('year', TIMESTAMPADD(YEAR, -1, CAST('{{ var("target_date") }}' AS DATE))) AS DATE)
            AND CAST(load_date AS DATE) <= CAST(TIMESTAMPADD(YEAR, -1, DATE_TRUNC('month', CAST('{{ var("target_date") }}' AS DATE))) AS DATE)
      )
)

SELECT * FROM curr
UNION ALL SELECT * FROM prev_period
UNION ALL SELECT * FROM yoy_period

{% else %}

SELECT * FROM curr

{% endif %}

ORDER BY load_date, credit_by_sector, type_period
