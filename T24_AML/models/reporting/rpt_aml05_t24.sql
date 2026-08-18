{{
    config(
        materialized='incremental',
        unique_key='report_id',
        incremental_strategy='merge',
        partition_by='dir0',
        tags=['aml', 'aml05', 'no_legal_doc', 'reporting'],
        database='hive',
        schema='gold'
    )
}}

/*
    Reporting model for AML05 - No Legal Document Customers Report
    Purpose: Report on customers with no or expired legal documents
    Source: hive.silver.t24_no_legal_doc_cus_report (direct read — adapters stg_aml05__accounts_t24
            and stg_aml05__customers_t24 are now orphaned; silver is already denormalized at
            {customer, account} grain).

    Template fields (AML05 registry) → output column mapping:
      BRANCH NAME       → branch_name
      CUSTOMER NUMBER   → customer_number
      ACCOUNT NUMBER    → account_number
      CUSTOMER NAME     → customer_name
      DATE OF BIRTH     → date_of_birth
      ACC OPEN DATE     → acc_open_date
      ID DOC TYPE       → id_doc_type
      ID DOC NUMBER     → id_doc_number
      EXPIRED DATE DOC  → expired_date_doc

    Logic from NO.LEGAL.DOC.CUS:
    - Filter customers with LEGAL.ID = "" (no legal ID) OR LEGAL.EXP.DATE <= TODAY (expired)
    - Link to their accounts
    - Include branch, customer, and account details

    Note: 35 rows post 2026-06-22 reconcile (32 blank-legal-id arm + 3 expired-doc arm accounts).
          expired_date_doc now maps from cus.legal_exp_date[0] — confirmed present in bronze t24_customer.
          id_doc_type and id_doc_number are NULL for blank-legal-id arm rows (by-design — no doc issued).

    branch_name: sourced directly from silver branch_name column (= BRANCH NAME on t24_no_legal_doc_cus_report).
    IMPORTANT: branch_name is NOT company_code — this matches the fix applied in stg_aml05__accounts_t24.

    Silver grain: one row per {customer_id, account_id}. No join needed — silver is denormalized.
    The former adapter pair stg_aml05__accounts_t24 + stg_aml05__customers_t24 joined silver to
    silver (same underlying table); here we read silver once and project all fields inline.
*/

with silver as (
    select *
    from hive.silver.t24_no_legal_doc_cus_report
    {% if is_incremental() %}
    where dir0 = REPLACE('{{ var("mis_date", "") }}', '-', '')
    {% endif %}
),

-- Final projection: output schema is byte-identical to former multi-adapter version
final as (
    select
        -- Unique identifier for the report
        {{ dbt_utils.generate_surrogate_key([
            'customer_id',
            'account_id',
            'dir0'
        ]) }} as report_id,

        -- Report metadata
        current_timestamp() as report_generated_at,

        -- -------------------------------------------------------
        -- AML05 template fields (snake_case; Dremio MERGE-safe)
        -- template field   → column name
        -- BRANCH NAME      → branch_name
        -- CUSTOMER NUMBER  → customer_number
        -- ACCOUNT NUMBER   → account_number
        -- CUSTOMER NAME    → customer_name
        -- DATE OF BIRTH    → date_of_birth
        -- ACC OPEN DATE    → acc_open_date
        -- ID DOC TYPE      → id_doc_type
        -- ID DOC NUMBER    → id_doc_number
        -- EXPIRED DATE DOC → expired_date_doc
        -- -------------------------------------------------------
        -- BRANCH NAME: sourced from silver branch_name (NOT company_code)
        trim(cast(branch_name as varchar))              as branch_name,

        trim(cast(customer_id as varchar))              as customer_number,
        trim(cast(account_id as varchar))               as account_number,
        trim(cast(customer_full_name as varchar))       as customer_name,

        -- DATE OF BIRTH: birth_or_incorp_date in silver (= date_of_birth)
        cast(birth_or_incorp_date as varchar)           as date_of_birth,

        -- ACC OPEN DATE: opening_date in silver (= open_account_date)
        cast(opening_date as varchar)                   as acc_open_date,

        -- ID DOC TYPE: legal_doc_type in silver (= id_doc_type)
        trim(cast(legal_doc_type as varchar))           as id_doc_type,

        -- ID DOC NUMBER: legal_id in silver (= id_doc_no)
        trim(cast(legal_id as varchar))                 as id_doc_number,

        -- EXPIRED DATE DOC: legal_doc_expiry_date in silver (= expired_date_doc)
        -- NULL in dev (LEGAL.EXP.DATE absent from bronze t24_customer scope)
        cast(legal_doc_expiry_date as varchar)          as expired_date_doc,

        -- -------------------------------------------------------
        -- Additional report columns (commented out 2026-06-22 — keep template fields only)
        -- -------------------------------------------------------
        -- trim(cast(customer_short_name as varchar))      as customer_short_name,
        -- has_no_legal_id                                 as has_no_legal_id,
        -- cast(is_legal_doc_expired as varchar)           as is_legal_doc_expired,
        -- requires_legal_doc_update                       as requires_legal_doc_update,
        -- trim(cast(account_category as varchar))         as account_category,
        -- trim(cast(account_currency as varchar))         as account_currency,
        -- cast(working_balance as double)                 as account_balance,
        -- trim(cast(gender as varchar))                   as customer_gender,
        -- trim(cast(occupation as varchar))               as customer_occupation,
        --
        -- -- Compliance flag
        -- requires_legal_doc_update                       as is_reportable,

        -- Partition field
        dir0                                            as dir0,

        -- Metadata
        current_timestamp()                             as loaded_at

    from silver
)

select * from final
