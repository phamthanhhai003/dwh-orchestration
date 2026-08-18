{{
    config(
        materialized='incremental',
        unique_key='report_id',
        incremental_strategy='merge',
        partition_by='dir0',
        tags=['aml', 'aml03', 'inactive_accounts', 'reporting'],
        database='hive',
        schema='gold'
    )
}}

/*
    Reporting model for AML03 - Inactive Accounts Report
    Purpose: Report on inactive accounts (no transactions in last 30 days)
    Source: hive.silver.t24_inactive_account_report (direct read — adapters stg_aml03__accounts_t24
            and stg_aml03__customers_t24 are now orphaned; silver is already denormalized).

    Template fields (AML03 registry) → output column mapping:
      BRANCH NAME      → branch_name
      ACCOUNT NUMBER   → account_number
      CUSTOMER NUMBER  → customer_number
      CUSTOMER NAME    → customer_name
      ACCOUNT CCY      → account_ccy
      LAST TXN DATE    → last_txn_date
      ACC BALANCE      → acc_balance
      ACC OPEN DATE    → acc_open_date

    Logic from INACTIV.ACCOUNT.DETAIL:
    - Filter accounts with INACTIV_MARKER = 'Y' (or boolean true in silver)
    - Filter accounts with no transactions in last 30 days (is_inactive_30_days)
    - Include branch, customer, and account details

    Silver grain: one row per (account_id, mis_date). All customer fields are pre-joined
    at silver build time; no cross-grain join needed here.

    Customer dedup note: the former stg_aml03__customers_t24 deduped to one row per
    (customer_id, dir0). In the denormalized silver, each row is an account row that carries
    the customer fields inline — so the join is 1:1 on account grain (same customer_full_name
    available on every account row).
*/

with silver as (
    select *
    from hive.silver."t24_inactive_account_report"
    {% if is_incremental() %}
    where dir0 = REPLACE('{{ var("mis_date") }}', '-', '')
    {% endif %}
),

-- Filter only inactive accounts (30+ days without transactions)
inactive_accounts as (
    select *
    from silver
    where (inactive_marker = 'Y' or inactive_marker = 'true' or cast(inactive_marker as varchar) = 'true')
),

-- Final projection: output schema is byte-identical to former multi-adapter version
final as (
    select
        -- Unique identifier for the report
        {{ dbt_utils.generate_surrogate_key([
            'account_id',
            'dir0'
        ]) }} as report_id,

        -- Report metadata
        current_timestamp() as report_generated_at,

        -- -------------------------------------------------------
        -- AML03 template fields (snake_case; Dremio MERGE-safe)
        -- template field  → column name
        -- BRANCH NAME     → branch_name
        -- ACCOUNT NUMBER  → account_number
        -- CUSTOMER NUMBER → customer_number
        -- CUSTOMER NAME   → customer_name
        -- ACCOUNT CCY     → account_ccy
        -- LAST TXN DATE   → last_txn_date
        -- ACC BALANCE     → acc_balance
        -- ACC OPEN DATE   → acc_open_date
        -- -------------------------------------------------------
        trim(cast(branch_name as varchar))              as branch_name,
        trim(cast(account_id as varchar))               as account_number,
        trim(cast(customer_id as varchar))              as customer_number,
        trim(cast(customer_full_name as varchar))       as customer_name,
        trim(cast(account_currency as varchar))         as account_ccy,
        cast(last_transaction_date as varchar)          as last_txn_date,
        cast(working_balance as double)                 as acc_balance,
        cast(opening_date as varchar)                   as acc_open_date,

        -- -------------------------------------------------------
        -- Additional report columns (commented out 2026-06-22 — keep template fields only)
        -- -------------------------------------------------------
        -- trim(cast(company_code as varchar))             as company_code,
        -- trim(cast(account_title_1 as varchar))          as account_title_1,
        -- trim(cast(account_title_2 as varchar))          as account_title_2,
        -- trim(cast(short_title as varchar))              as account_short_title,
        -- trim(cast(account_category as varchar))         as account_category,
        -- cast(last_credit_date as varchar)               as last_credit_date,
        -- cast(last_debit_date as varchar)                as last_debit_date,
        -- days_since_last_txn                             as days_since_last_txn,
        -- cast(inactive_marker as varchar)                as inactive_marker,
        -- cast(is_inactive_30_days as varchar)            as is_inactive_30_days,
        -- trim(cast(customer_short_name as varchar))      as customer_short_name,
        -- trim(cast(legal_id as varchar))                 as customer_legal_id,
        -- trim(cast(legal_doc_type as varchar))           as customer_legal_doc_type,

        -- Partition field
        dir0                                            as dir0,

        -- Metadata
        current_timestamp()                             as loaded_at

    from inactive_accounts
)

select * from final
