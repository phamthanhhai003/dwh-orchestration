{{
    config(
        materialized='incremental',
        unique_key='report_id',
        incremental_strategy='merge',
        partition_by='dir0',
        tags=['aml', 'aot', 'accounts_opened', 'reporting'],
        database='hive',
        schema='gold'
    )
}}

/*
    Reporting model for AOT - Accounts Opened Today report
    Purpose: One row per account opened on the reporting date, with full KYC identity.
    Source: hive.silver.t24_e_account_open (ENQ E.ACCOUNT.OPEN; direct read — silver is
            already denormalized at account grain with company + customer fields joined).

    Template fields (AOT_template2.xlsx) -> output column mapping:
      BRANCH NUMBER       -> branch_number       (silver branch_no = co_code)
      BRANCH NAME         -> branch_name         (silver branch_name = company_name)
      ACCOUNT NUMBER      -> account_number      (silver acct_no)
      CUSTOMER NUMBER     -> customer_number     (silver cus_no)
      CUSTOMER NAME       -> customer_name       (silver cus_name)
      OPENING DATE        -> opening_date        (silver open_acct_date)
      ID DOC TYPE         -> id_doc_type         (silver ident_doc_type)
      ID DOC NUMBER       -> id_doc_number       (silver ident_doc_no)
      ALTER ID TYPE       -> alter_id_type       (silver alter_id_type)
      ALTER ID NUMBER     -> alter_id_number     (silver alter_id_no)
      PEP                 -> pep                 (silver pep)
      DOB                 -> dob                 (silver dob)
      NATIONALITY         -> nationality         (silver nationality)
      BIRTH COUNTRY       -> birth_country       (silver birth_country)
      ADDRESS             -> address             (silver address)
      OCCUPATION          -> occupation          (silver occupation)
      ACCOUNT BALANCE     -> account_balance     (silver acct_bal)
      FIRST DEPOSIT AMOUNT-> first_deposit_amount(silver first_deposit_amt)

    Logic from E.ACCOUNT.OPEN (T24_FIELD_LOGIC_REGISTRY.md Part 6 section 7):
      Driver = accounts whose OPENING.DATE = the requested date (silver already applies this
      via var target_date at silver build time). Identity, doc types, PEP, address, occupation
      come from CUSTOMER; branch from COMPANY; first deposit = first positive STMT.ENTRY.

    Note: Dev 0-row result expected — t24_e_account_open is empty when no account was opened on
          the target_date snapshot (T24_SOURCE_RECONCILE.md note 4). Schema is still emitted.

    Partition: silver has no dir0 column; derive dir0 = YYYYMMDD from open_acct_date
               (same pattern as t24_daily_txn_9k silver).
*/

with silver as (
    select
        *,
        replace(cast(open_acct_date as varchar), '-', '') as dir0
    from hive.silver.t24_e_account_open
    {% if is_incremental() %}
    where replace(cast(open_acct_date as varchar), '-', '')
          = replace('{{ var("mis_date", "") }}', '-', '')
    {% endif %}
),

final as (
    select
        -- Unique identifier for the report
        {{ dbt_utils.generate_surrogate_key([
            'acct_no',
            'cus_no',
            'open_acct_date'
        ]) }} as report_id,

        -- Report metadata
        current_timestamp() as report_generated_at,

        -- -------------------------------------------------------
        -- AOT template fields (snake_case; Dremio MERGE-safe), in template order
        -- -------------------------------------------------------
        trim(cast(branch_no as varchar))                as branch_number,
        trim(cast(branch_name as varchar))              as branch_name,
        trim(cast(acct_no as varchar))                  as account_number,
        trim(cast(cus_no as varchar))                   as customer_number,
        trim(cast(cus_name as varchar))                 as customer_name,
        cast(open_acct_date as varchar)                 as opening_date,
        trim(cast(ident_doc_type as varchar))           as id_doc_type,
        trim(cast(ident_doc_no as varchar))             as id_doc_number,
        trim(cast(alter_id_type as varchar))            as alter_id_type,
        trim(cast(alter_id_no as varchar))              as alter_id_number,
        trim(cast(pep as varchar))                      as pep,
        cast(dob as varchar)                            as dob,
        trim(cast(nationality as varchar))              as nationality,
        trim(cast(birth_country as varchar))            as birth_country,
        trim(cast(address as varchar))                  as address,
        trim(cast(occupation as varchar))               as occupation,
        cast(acct_bal as double)                        as account_balance,
        cast(first_deposit_amt as double)               as first_deposit_amount,

        -- Partition field: YYYYMMDD derived from opening date (silver has no dir0)
        dir0                                            as dir0,

        -- Metadata
        current_timestamp()                             as loaded_at

    from silver
)

select * from final
