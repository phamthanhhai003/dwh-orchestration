{{ config(materialized='table', schema='silver') }}

-- total_accounts cho LIABILITIES report = COUNT(DISTINCT account_number) per gl_line từ CRB
-- (hive.bronze.crb_bnctlgl — deposit accounts, native T24 SFTP). Khớp memory project_crb_parser.
-- Khoá XUẤT = (load_date, co_code, gl_line). Gold final_liabilities join theo co_code + line.
--
-- ⚠️ crb_bnctlgl.branch_name = MÃ 3 KÝ TỰ (GLN/LQC/MTT/SUA...) KHÁC branch_name của
--    trial_balance (GLENO/LIQUICA...). Khoá chung DUY NHẤT = co_code → map tĩnh mã CRB → co_code.
--
-- ⚠️ Đếm theo TỪNG PHÂN KHÚC deposit (gl_line đầy đủ) — CHỈ các line thật, LOẠI group-total
--    (project_crb_parser): demand=4310/4320/4340, time=4380/4390/4400/4410,
--    passbook=4450/4460/4470/4475/4480/4490/4500/4505/4506/4510/4520; group total
--    4360/4430/4540 = CHỈ reconcile, KHÔNG đếm. Lọc qua deposit_type (parser đã phân loại theo
--    đúng list gl_line trên) → tự loại group-total + line ngoài phân khúc.

{% set run_date = var('business_date') %}

WITH crb AS (
    SELECT
        business_date AS load_date,
        CASE branch_name
            WHEN 'BNK' THEN 'TL0010001' WHEN 'DIL' THEN 'TL0010002'
            WHEN 'GLN' THEN 'TL0010003' WHEN 'MAL' THEN 'TL0010004'
            WHEN 'ALU' THEN 'TL0010005' WHEN 'OCS' THEN 'TL0010006'
            WHEN 'BCU' THEN 'TL0010007' WHEN 'SAM' THEN 'TL0010008'
            WHEN 'ANR' THEN 'TL0010009' WHEN 'SUA' THEN 'TL0010010'
            WHEN 'VQQ' THEN 'TL0010011' WHEN 'LSP' THEN 'TL0010012'
            WHEN 'LQC' THEN 'TL0010013' WHEN 'MTT' THEN 'TL0010014'
            ELSE NULL
        END AS co_code,
        CAST(gl_line AS VARCHAR) AS line,
        deposit_type,
        account_number
    FROM hive.bronze.crb_bnctlgl
    WHERE business_date = DATE '{{ run_date }}'
      AND deposit_type IN ('demand', 'time', 'passbook')   -- chỉ phân khúc deposit thật
      AND CAST(gl_line AS VARCHAR) NOT IN ('4360', '4430', '4540')  -- loại group total (defense)
)

SELECT
    load_date,
    co_code,
    line,
    COUNT(DISTINCT account_number) AS total_accounts
FROM crb
WHERE co_code IS NOT NULL
GROUP BY load_date, co_code, line
