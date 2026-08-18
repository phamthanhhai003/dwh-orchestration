{{ config(
    materialized='table',
    unique_key=['report_month'],
    incremental_strategy='merge',
    database='hive',
    schema='gold',
    tags=['soc', 'monthly', 'liquidity_report']
) }}

WITH asset_base AS (
    SELECT
        report_month,
        code,
        description,
        amount
    FROM {{ ref('soc_assets_real_v2') }}
    {% if var("target_date", "") != "" %}
    WHERE report_month = '{{ var("target_date")[0:7] }}'
    {% elif is_incremental() %}
    WHERE report_month >= (
        SELECT COALESCE(MAX(report_month), '1900-01')
        FROM {{ this }}
    )
    {% endif %}
),

liab_base AS (
    SELECT
        report_month,
        code,
        description,
        amount
    FROM {{ ref('soc_liabilities_real_v2') }}
    {% if var("target_date", "") != "" %}
    WHERE report_month = '{{ var("target_date")[0:7] }}'
    {% elif is_incremental() %}
    WHERE report_month >= (
        SELECT COALESCE(MAX(report_month), '1900-01')
        FROM {{ this }}
    )
    {% endif %}
),

asset_components AS (
    SELECT
        report_month,

        SUM(
            CASE
                WHEN description IN ('Cash in Vault', 'Cash in ATM Vault')
                THEN amount ELSE 0
            END
        ) AS vault_cash,

        CAST(0 AS DOUBLE) AS precious_metals,

        SUM(
            CASE
                WHEN description IN (
                    'Due from Central Bank',
                    'Item in course of collect',
                    'Item in course of collect.',
                    'Items in course of collect.'
                )
                THEN amount ELSE 0
            END
        )
        + SUM(
            CASE
                WHEN description = 'Foreign Currency'
                THEN amount ELSE 0
            END
        ) AS deposits_with_bctl,

        SUM(
            CASE
                WHEN code IN ('I23', 'I28') THEN amount
                WHEN description = 'Call Deposits' THEN amount
                WHEN code = '1.01.4.1.2' AND description = 'OTHERS' THEN amount
                WHEN code = '1.01.4.3' THEN amount
                WHEN description = 'Foreign Currency' THEN amount
                ELSE 0
            END
        ) AS deposits_in_other_fi,

        CAST(0 AS DOUBLE) AS readily_marketable_securities,

        CAST(0 AS DOUBLE) AS net_interbank_lending_borrowing_1m

    FROM asset_base
    GROUP BY report_month
),

liab_components AS (
    SELECT
        report_month,

        SUM(
            CASE
                WHEN description IN (
                    'Demand Deposit',
                    'Time Deposits',
                    'Other (Passbook Saving)',
                    'Other Sundry Current Liabilit.',
                    'Other liabilit/Due To Branch',
                    'Other Liabilit/Due To Branch'
                )
                OR code IN ('I201', 'I217', 'I233')
                THEN amount
                ELSE 0
            END
        ) AS total_liabilities

    FROM liab_base
    GROUP BY report_month
),

final_data AS (
    SELECT
        a.report_month,
        a.vault_cash,
        a.precious_metals,
        a.deposits_with_bctl,
        a.deposits_in_other_fi,
        a.readily_marketable_securities,
        a.net_interbank_lending_borrowing_1m,

        (
            COALESCE(a.vault_cash, 0)
            + COALESCE(a.precious_metals, 0)
            + COALESCE(a.deposits_with_bctl, 0)
            + COALESCE(a.deposits_in_other_fi, 0)
            + COALESCE(a.readily_marketable_securities, 0)
            + COALESCE(a.net_interbank_lending_borrowing_1m, 0)
        ) AS total_highly_liquid_assets,

        COALESCE(l.total_liabilities, 0) AS total_liabilities

    FROM asset_components a
    LEFT JOIN liab_components l
        ON a.report_month = l.report_month
)

SELECT
    report_month,
    vault_cash AS Vault_Cash,
    precious_metals AS Precious_metals,
    deposits_with_bctl AS Deposits_with_BCTL,
    deposits_in_other_fi AS Deposits_in_Other_Financial_Institutions,
    readily_marketable_securities AS Readily_Marketable_Securities,
    net_interbank_lending_borrowing_1m AS Net_inter_bank_lending_borrowing_1_month,
    total_highly_liquid_assets AS Total_Highly_Liquid_Assets,
    total_liabilities AS Total_Liabilities,
    total_highly_liquid_assets / NULLIF(total_liabilities, 0) AS Liquidity_Ratio,
    CONCAT(
        CASE CAST(SUBSTR(report_month, 6, 2) AS INTEGER)
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
        SUBSTR(report_month, 1, 4)
    ) AS report_month_label
FROM final_data
ORDER BY report_month DESC
