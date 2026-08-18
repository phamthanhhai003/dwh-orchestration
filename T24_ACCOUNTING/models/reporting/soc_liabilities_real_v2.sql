{{ config(
    materialized='table', 
    database='hive',
    schema='gold',
    tags=['soc', 'monthly']
) }}

WITH all_months AS (
    SELECT DISTINCT
        CAST(DATE_TRUNC('month', CAST(load_date AS DATE)) AS DATE) AS month_start
    FROM {{ ref('final_liabilities_report') }}
    {% if var("target_date", "") != "" %}
    WHERE CAST(load_date AS DATE) >= CAST(DATE_TRUNC('month', CAST('{{ var("target_date") }}' AS DATE)) AS DATE)
      AND CAST(load_date AS DATE) < CAST(TIMESTAMPADD(MONTH, 1, DATE_TRUNC('month', CAST('{{ var("target_date") }}' AS DATE))) AS DATE)
    {% endif %}
),

periods AS (
    -- Nhánh 1: Kỳ hiện tại (CURR)
    SELECT 
        CAST(month_start AS DATE)                                       AS period_start, 
        month_start                                                     AS report_month_start, 
        'CURR'                                                          AS period_type, 
        3                                                               AS period_order 
    FROM all_months
    
    UNION ALL
    
    -- Nhánh 2: Kỳ tháng trước (PREV)
    SELECT 
        CAST(TIMESTAMPADD(MONTH, -1, month_start) AS DATE)              AS period_start, 
        month_start, 
        'PREV', 
        2 
    FROM all_months
    
    UNION ALL
    
    -- Nhánh 3: Kỳ năm trước (YOY)
    SELECT 
        CAST(TIMESTAMPADD(YEAR, -1, month_start) AS DATE)               AS period_start, 
        month_start, 
        'YOY', 
        1 
    FROM all_months
),

order_spine AS (
    SELECT DISTINCT order_id, description FROM {{ ref('final_liabilities_report') }}
),

period_order_spine AS (
    SELECT 
        o.order_id,
        o.description, 
        p.period_start,
        p.report_month_start,
        p.period_type,
        p.period_order
    FROM order_spine o
    CROSS JOIN periods p
),

code_dim AS (
    SELECT DISTINCT
        order_id,
        description,
        code
    FROM {{ ref('final_liabilities_report') }}
),

base AS (
    SELECT
        TO_CHAR(pos.report_month_start, 'YYYY-MM')                      AS report_month,
        pos.order_id,
        pos.description,
        pos.period_type,
        pos.period_order,
        COALESCE(SUM(f.amount), 0)                                      AS amount,
        COALESCE(SUM(f.total_accounts), 0)                              AS accts
    FROM period_order_spine pos
    LEFT JOIN {{ ref('final_liabilities_report') }} f
      ON pos.order_id = f.order_id
     AND pos.description = f.description
     AND CAST(f.load_date AS DATE) >= pos.period_start
     AND CAST(f.load_date AS DATE) <  CAST(TIMESTAMPADD(MONTH, 1, pos.period_start) AS DATE)
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    b.report_month,
    d.code,
    b.order_id,
    b.description,
    b.period_type                                                       AS type_period,
    b.accts,
    b.amount,

    b.amount
        - LAG(b.amount) OVER (
            PARTITION BY b.report_month, b.order_id, b.description
            ORDER BY b.period_order
          )                                                             AS increase_prev_period,

    CASE
        WHEN LAG(b.amount) OVER (
            PARTITION BY b.report_month, b.order_id, b.description
            ORDER BY b.period_order
        ) = 0 THEN NULL
        ELSE (
            b.amount
            - LAG(b.amount) OVER (
                PARTITION BY b.report_month, b.order_id, b.description
                ORDER BY b.period_order
            )
        )
        / LAG(b.amount) OVER (
            PARTITION BY b.report_month, b.order_id, b.description
            ORDER BY b.period_order
        )
    END                                                                 AS pct_change_prev_period,

    CONCAT(
        CASE CAST(SUBSTR(b.report_month, 6, 2) AS INTEGER)
            WHEN 1  THEN 'January'
            WHEN 2  THEN 'February'
            WHEN 3  THEN 'March'
            WHEN 4  THEN 'April'
            WHEN 5  THEN 'May'
            WHEN 6  THEN 'June'
            WHEN 7  THEN 'July'
            WHEN 8  THEN 'August'
            WHEN 9  THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END,
        ' ',
        SUBSTR(b.report_month, 1, 4)
    ) AS report_month_label

FROM base b
LEFT JOIN code_dim d
  ON b.order_id = d.order_id
 AND b.description = d.description
ORDER BY
    b.report_month          DESC,
    CAST(b.order_id AS INTEGER) ASC,
    b.period_order          ASC