{{
    config(
        materialized='incremental',
        unique_key='report_id',
        incremental_strategy='merge',
        partition_by='dir0',
        tags=['aml', '9k_transaction', 'reporting'],
        database='hive',
        schema='gold'
    )
}}

/*
    Reporting model for 9K Transaction Report
    Purpose: Aggregate transactions where the total daily amount per account is >= 9000 USD.
    Source: hive.silver.t24_daily_txn_9k (direct read — adapters stg_9k_transaction__accounts_t24,
            stg_9k_transaction__customers_t24, and stg_9k_transaction__txn_entry_t24 are now
            orphaned; silver is already denormalized at txn-entry grain with account+customer fields).

    Template fields (9k registry) → output column mapping:
      Date               → txn_date
      Acc Number         → acc_number
      Acc Name           → acc_name
      Type Of Tranzation → type_of_tranzation
      Amount             → amount
      Branch             → branch

    Logic from BNCTL.DAILY.TXN.9K:
    - Only include USD currency transactions
    - If total daily transactions per account >= 9000 USD, list all constituent transactions in detail
    - Include customer and account details

    Note: Dev 0-row result expected — silver t24_daily_txn_9k has no data in dev (empty snapshot).

    Silver grain: one row per txn entry. Account fields (account_title_1, account_title_2,
    short_title, account_category, account_currency, working_balance) and customer fields
    (customer_full_name, customer_short_name, customer_id) are pre-joined at silver build time.
    The three former adapters read the same silver table; this model reads it once.

    Column mapping from silver → former adapter aliases:
      silver.account_id             → hva.account_id  (txn adapter)
      silver.transaction_id         → hva.transaction_id
      silver.application_id         → hva.application_id
      silver.branch_code            → hva.branch_code
      silver.branch_name            → hva.branch_name
      silver.company_code           → hva.company_code
      silver.account_customer_id    → hva.account_customer_id
      silver.operation_date         → hva.operation_date
      silver.value_date             → hva.value_date
      silver.transaction_currency   → hva.transaction_currency
      silver.amount (ABS lcy)       → hva.amount_currency
      silver.amount_ref_currency    → hva.amount_ref_currency
      silver.acct_name              → hva.account_name
      silver.cust_name              → hva.customer_name
      silver.debit_credit_indicator → hva.debit_credit_indicator
      silver.back_office_name       → hva.back_office_name  (NULL; documented gap)
      silver.dir0                   → hva.dir0
      silver.transaction_approval_timestamp → hva.transaction_approval_timestamp
      silver.account_title_1        → a.account_title_1  (former accounts adapter)
      silver.account_title_2        → a.account_title_2
      silver.short_title            → a.short_title
      silver.account_category       → a.account_category
      silver.account_currency       → a.account_currency
      silver.working_balance        → a.working_balance
      silver.customer_full_name     → c.customer_full_name  (former customers adapter)
      silver.customer_short_name    → c.customer_short_name
      silver.customer_id            → c.customer_id
*/

with silver as (
    select *
    from hive.silver.t24_daily_txn_9k
    {% if is_incremental() %}
    where dir0 = REPLACE('{{ var("mis_date") }}', '-', '')
    {% endif %}
),

-- Aggregate transactions by account and date (was applied to stg_9k_transaction__txn_entry_t24)
-- Fix (2026-06-22): amount_ref_currency maps to t24_stmt_entry.amount_fcy which is not ingested
-- in bronze (0 rows in hive.bronze.t24_stmt_entry.amount_fcy). Use silver.amount (= ABS(amount_lcy))
-- instead. amount_lcy is fully populated. Registry §14: 9K threshold is on LCY total daily movement.
account_daily_aggregates as (
    select
        *,
        count(*) over (
            partition by account_id, operation_date
        ) as transaction_count,
        sum(abs(amount)) over (
            partition by account_id, operation_date
        ) as total_amount,
        sum(case when debit_credit_indicator = 'Debit' then abs(amount) else 0 end) over (
            partition by account_id, operation_date
        ) as total_debit,
        sum(case when debit_credit_indicator = 'Credit' then abs(amount) else 0 end) over (
            partition by account_id, operation_date
        ) as total_credit
    from silver
    where transaction_currency = 'USD'
),

-- Filter accounts with total >= 9000 USD
high_value_accounts as (
    select *
    from account_daily_aggregates
    where total_amount >= 9000
),

-- Final projection: output schema is byte-identical to former multi-adapter version
final as (
    select
        -- Unique identifier for the report
        {{ dbt_utils.generate_surrogate_key([
            'hva.account_id',
            'hva.operation_date',
            'hva.transaction_id'
        ]) }} as report_id,

        -- Report metadata
        current_timestamp() as report_generated_at,

        -- -------------------------------------------------------
        -- 9k template fields (snake_case; Dremio MERGE-safe)
        -- template field       → column name
        -- Date                 → txn_date
        -- Acc Number           → acc_number
        -- Acc Name             → acc_name
        -- Type Of Tranzation   → type_of_tranzation
        -- Amount               → amount
        -- Branch               → branch
        -- -------------------------------------------------------
        -- Date: operation_date (booking date of the transaction)
        cast(hva.operation_date as varchar)             as txn_date,

        -- Acc Number: account_id from txn entry (= account number)
        trim(cast(hva.account_id as varchar))           as acc_number,

        -- Acc Name: acct_name from silver (ACCOUNT.TITLE.1 + SHORT.TITLE)
        trim(cast(hva.acct_name as varchar))            as acc_name,

        -- Type Of Tranzation: transaction DESCRIPTION (template shows "Cash Deposit",
        -- "Cheque Encashment" etc). Silver txn_desc = TRANSACTION.stmt_narr; fall back to the
        -- raw application_id code only when the description is missing.
        trim(cast(coalesce(nullif(trim(cast(hva.txn_desc as varchar)), ''),
                           cast(hva.application_id as varchar)) as varchar)) as type_of_tranzation,

        -- Amount: silver.amount = ABS(amount_lcy) per txn row (amount_ref_currency=amount_fcy
        -- is not ingested in bronze; amount_lcy is fully populated — Registry §14 threshold).
        cast(hva.amount as double)                      as amount,

        -- Branch: branch_name (decoded from co_code in silver)
        trim(cast(hva.branch_name as varchar))          as branch,

        -- -------------------------------------------------------
        -- Additional report columns (commented out 2026-06-22 — keep template fields only)
        -- -------------------------------------------------------
        -- hva.transaction_count                           as transaction_count,
        -- cast(hva.total_amount as double)                as total_amount,
        -- cast(hva.total_debit as double)                 as total_debit_amount,
        -- cast(hva.total_credit as double)                as total_credit_amount,
        -- trim(cast(hva.transaction_id as varchar))       as transaction_id,
        -- trim(cast(hva.branch_code as varchar))          as branch_code,
        -- trim(cast(hva.transaction_currency as varchar)) as transaction_currency,
        -- trim(cast(hva.debit_credit_indicator as varchar)) as debit_credit_indicator,
        --
        -- -- Account details (from silver — formerly from accounts adapter)
        -- trim(cast(hva.account_title_1 as varchar))      as account_title_1,
        -- trim(cast(hva.account_title_2 as varchar))      as account_title_2,
        -- trim(cast(hva.short_title as varchar))          as account_short_title,
        -- trim(cast(hva.account_category as varchar))     as account_category,
        -- trim(cast(hva.account_currency as varchar))     as account_currency,
        -- cast(hva.working_balance as double)             as account_balance,
        --
        -- -- Customer details (from silver — formerly from customers adapter)
        -- trim(cast(hva.account_customer_id as varchar))  as customer_id,
        -- trim(cast(hva.customer_full_name as varchar))   as customer_full_name,
        -- trim(cast(hva.customer_short_name as varchar))  as customer_short_name,
        --
        -- -- Compliance flag
        -- case when hva.total_amount >= 9000 then 'Y' else 'N' end as is_reportable,

        -- Partition field
        hva.dir0                                        as dir0,

        -- Metadata
        current_timestamp()                             as loaded_at

    from high_value_accounts hva
    where hva.debit_credit_indicator is not null
)

select * from final
