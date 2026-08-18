-- demo_deposit_by_sector — REPORT DEMO trộn ĐỦ 3 luồng (chứng minh E2E design v1.4).
--   CDC  : t24_account, t24_customer  → đọc AT SNAPSHOT (var snap_* từ etl_control, gate truyền vào)
--   PULL : t24_sector                 → đọc WHERE business_date = D
--   SFTP : crb_deposits               → đọc WHERE load_date = D
-- Gold: mỗi sector → tổng số dư tiền gửi + #account + #customer, stamp business_date=D.
--
-- ⚠️ TODO verify khi cluster khoẻ (Dremio): tên cột thật + join key.
--   - crb_deposits.account_number CÓ khớp t24_account.recid không? (account_number đã xử lý
--     branch/product code → có thể cần chuẩn hoá trước khi join). Đây là rủi ro join chính.
--   - t24_account.customer / t24_customer.sector / t24_sector.recid+description: xác nhận tên cột
--     parser sinh ra (lowercase field T24). Sửa lại bên dưới nếu lệch.
{{ config(materialized='table', schema='gold') }}

with deposits as (
    -- SFTP: số dư tiền gửi theo account, cho ngày D
    select
        account_number,
        local_ccy_amt
    from hive.bronze.crb_deposits
    where load_date = date '{{ var("business_date") }}'
),

account as (
    -- CDC: account → customer (đóng băng tại snapshot pin lúc gate)
    select recid as account_id, customer as customer_id
    from hive.bronze.t24_account at snapshot '{{ var("snap_account") }}'
),

customer as (
    -- CDC: customer → sector code
    select recid as customer_id, sector as sector_code
    from hive.bronze.t24_customer at snapshot '{{ var("snap_customer") }}'
),

sector as (
    -- PULL: sector code → tên (cho ngày D)
    select recid as sector_code, description as sector_name
    from hive.bronze.t24_sector
    where business_date = date '{{ var("business_date") }}'
),

joined as (
    select
        s.sector_code,
        s.sector_name,
        d.account_number,
        c.customer_id,
        d.local_ccy_amt
    from deposits d
    join account  a on d.account_number = a.account_id   -- ⚠️ join key cần verify
    join customer c on a.customer_id     = c.customer_id
    join sector   s on c.sector_code     = s.sector_code
)

select
    sector_code,
    sector_name,
    cast('{{ var("business_date") }}' as date)   as business_date,
    sum(local_ccy_amt)                           as total_deposit_local,
    count(distinct account_number)               as n_accounts,
    count(distinct customer_id)                  as n_customers
from joined
group by sector_code, sector_name
