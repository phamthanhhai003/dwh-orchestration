{{ config(
    materialized='table',
    database='hive',
    schema='silver'
) }}

-- Division: accounting | Source: CRF native T24 (SFTP HOLD manifest → bronze)
--   GL lines  -> hive.bronze.crf_bnctlgl  (assets / liabilities / capital — balance sheet)
--   PL lines  -> hive.bronze.crf_bnctlpl  (income / expense — profit & loss)
-- Cả 2 lọc WHERE business_date = '{{ var("business_date") }}' (flow=sftp, seal theo D).
--
-- Trial balance T24 = report text phẳng: mỗi report (1 file = 1 branch + 1 ledger) gồm
-- HEADER line (line_code 4 chữ số, mang số dư GL line) xen kẽ DETAIL row (line_code rỗng =
-- 1 GL account "AC.../PL..." parse từ description). Quan hệ cha-con:
--   ⚠️ MẸ của 1 detail row là HEADER line NGAY BÊN DƯỚI nó (không phải bên trên) —
--      đã verify trên data (vd AC.1.TR.USD.3004...  -> line 2730 ngay dưới).
--   → group theo line_seq (số dòng trong file, parser ghi) với frame
--     CURRENT ROW..UNBOUNDED FOLLOWING; parent_line = MAX(line_code) trong group (header dưới).
-- line_seq reset theo TỪNG file nên partition PHẢI gồm source_file (1 branch có thể >1 file).
--
-- Dùng để:
--   * số dư báo cáo  : lấy từ HEADER row (original_line NOT NULL), gom theo line ở gold.
--   * total_accounts : KHÔNG còn đếm detail rows (cách cũ). Detail rows giờ chỉ cấp
--     product_key per header line → gold ASSET join CRIS đếm loan_id; gold LIAB đếm CRB.
-- (Bỏ cột chết final_line_id/hậu tố 'A' và total_accounts của bản cũ.)

{% set run_date = var('business_date') %}

WITH source_data AS (
    SELECT
        business_date                                   AS load_date,
        REGEXP_REPLACE(branch, '^.*_', '')              AS branch_name,
        -- co_code = map branch CRF → CRIS company_code (TL001 + số _NN_ pad 4). Branch không
        -- có _NN_ (ATAURO/UAT_ENV/Head Office) → NULL → asset count CRIS = 0 (theo quyết định).
        CASE WHEN REGEXP_LIKE(branch, '.*_[0-9]{2}_.*')
             THEN CONCAT('TL001', LPAD(REGEXP_EXTRACT(branch, '_([0-9]{2})_', 1), 4, '0'))
             ELSE NULL END                              AS co_code,
        REGEXP_REPLACE(file_type, '^.*_', '')           AS statement_type,   -- GL | PL
        source_file,
        line_seq,
        NULLIF(CAST(line_code AS VARCHAR), '')          AS original_line,
        description,
        opening_balance,
        debit_movements,
        credit_movements,
        closing_balance
    FROM (
        SELECT line_code, description, opening_balance, debit_movements,
               credit_movements, closing_balance, source_file, branch, file_type,
               line_seq, business_date
        FROM hive.bronze.crf_bnctlgl
        WHERE business_date = DATE '{{ run_date }}'
        UNION ALL
        SELECT line_code, description, opening_balance, debit_movements,
               credit_movements, closing_balance, source_file, branch, file_type,
               line_seq, business_date
        FROM hive.bronze.crf_bnctlpl
        WHERE business_date = DATE '{{ run_date }}'
    )
),

-- grp_id = số header (line_code không rỗng) tính từ dòng hiện tại XUỐNG cuối file.
-- Detail rows phía trên 1 header chia chung grp_id với header đó (mẹ ở dưới).
calc_groups AS (
    SELECT
        *,
        COUNT(original_line) OVER (
            PARTITION BY load_date, branch_name, statement_type, source_file
            ORDER BY line_seq
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS line_group_id
    FROM source_data
),

final_assigned AS (
    SELECT
        *,
        MAX(original_line) OVER (
            PARTITION BY load_date, branch_name, statement_type, source_file, line_group_id
        ) AS parent_line          -- header NGAY DƯỚI nhóm detail
    FROM calc_groups
)

SELECT
    load_date,
    branch_name,
    co_code,
    statement_type,
    source_file,
    line_seq,
    original_line,
    parent_line,
    description,
    opening_balance,
    debit_movements,
    credit_movements,
    closing_balance,

    -- product_key (= T24 category/product của detail account) + aging, parse từ description.
    -- Chỉ điền cho DETAIL row (original_line IS NULL). Dùng để map header→CRIS (asset count).
    --   AC row: AC.<co>.<src>.<ccy>.<PRODUCT=part5>...<AGING=part10>...
    --   PL row: PL.<PRODUCT=part2>...<AGING=part9>...
    CASE
        WHEN original_line IS NOT NULL THEN NULL
        WHEN SPLIT_PART(description, '.', 1) = 'PL'
            THEN NULLIF(SPLIT_PART(description, '.', 3), '')
        WHEN SPLIT_PART(description, '.', 1) = 'AC'
            THEN NULLIF(SPLIT_PART(description, '.', 5), '')
        ELSE NULL
    END AS product_key,

    CASE
        WHEN original_line IS NOT NULL THEN NULL
        WHEN SPLIT_PART(description, '.', 1) = 'PL'
            THEN NULLIF(SPLIT_PART(description, '.', 9), '')
        WHEN SPLIT_PART(description, '.', 1) = 'AC'
            THEN NULLIF(SPLIT_PART(description, '.', 10), '')
        ELSE NULL
    END AS loan_aging_status,

    CASE
        WHEN original_line IS NOT NULL THEN NULL
        WHEN SPLIT_PART(description, '.', 1) = 'PL'
            THEN NULLIF(SPLIT_PART(description, '.', 2), '')
        ELSE NULL
    END AS pl_account

FROM final_assigned
ORDER BY load_date, branch_name, statement_type, source_file, line_seq
