{{ config(
    materialized = 'table',
    database = 'hive',
    schema = 'gold',
    tags = ['remittance']
) }}

-- Division: treasury | Report: BNCTL.FT.INREMIT quarterly/monthly remittance rollup
-- Reads hive.silver.t24_ft_inremit DIRECTLY (no staging layer) — month-bucket grain change
-- + 12-month spine + TOTAL row done inline here. Repointed 2026-06-24 from the legacy
-- ref('stg_remittance_quarterly') (parser/mock chain) to real T24 silver.
--
-- Silver column mapping (hive.silver.t24_ft_inremit -> aggregation inputs):
--   transaction_ref  -> COUNT DISTINCT -> "SWIFT Number"
--   credit_amount    -> SUM            -> "Value (USD)"
--   charges          -> SUM            -> "Fees"
--   date_transfer    -> EXTRACT(MONTH/YEAR) for month/year bucketing
--
-- GAP: charges is CAST(NULL AS DOUBLE) in silver (TOTAL.CHARGE.AMT absent from bronze
--   t24_funds_transfer) -> "Fees" stays 0.0 until a charge source lands. Same gap as daily.

{% set year_filter %}
{% if var('report_year', none) is not none %}{{ var('report_year') }}{% elif var('target_date', none) is not none %}EXTRACT(YEAR FROM TO_DATE('{{ var("target_date") }}', 'YYYY-MM-DD')){% else %}YEAR(CURRENT_DATE){% endif %}
{% endset %}

WITH MonthSeries AS (
    SELECT 1 AS MonthNum, 'January'   AS MonthName UNION ALL
    SELECT 2, 'February'  UNION ALL
    SELECT 3, 'March'     UNION ALL
    SELECT 4, 'April'     UNION ALL
    SELECT 5, 'May'       UNION ALL
    SELECT 6, 'June'      UNION ALL
    SELECT 7, 'July'      UNION ALL
    SELECT 8, 'August'    UNION ALL
    SELECT 9, 'September' UNION ALL
    SELECT 10, 'October'  UNION ALL
    SELECT 11, 'November' UNION ALL
    SELECT 12, 'December'
),

ActualData AS (
    SELECT
        EXTRACT(MONTH FROM CAST(date_transfer AS DATE))               AS MonthNum,
        COUNT(DISTINCT transaction_ref)                                AS SwiftNum,
        SUM(credit_amount)                                             AS ValUSD,
        SUM(charges)                                                   AS TotalFees
    FROM hive.silver.t24_ft_inremit
    WHERE EXTRACT(YEAR FROM CAST(date_transfer AS DATE)) = {{ year_filter }}
    GROUP BY EXTRACT(MONTH FROM CAST(date_transfer AS DATE))
),

Monthly AS (
    SELECT
        m.MonthNum,
        m.MonthName                     AS "Month",
        COALESCE(d.SwiftNum,   0)       AS "SWIFT Number",
        COALESCE(d.ValUSD,     0.0)     AS "Value (USD)",
        COALESCE(d.TotalFees,  0.0)     AS "Fees"
    FROM MonthSeries m
    LEFT JOIN ActualData d ON m.MonthNum = d.MonthNum
)

SELECT "Month", "SWIFT Number", "Value (USD)", "Fees"
FROM (
    SELECT "Month", "SWIFT Number", "Value (USD)", "Fees", MonthNum
    FROM Monthly

    UNION ALL

    SELECT
        'TOTAL',
        SUM("SWIFT Number"),
        SUM("Value (USD)"),
        SUM("Fees"),
        13
    FROM Monthly
) FinalResult
ORDER BY MonthNum
