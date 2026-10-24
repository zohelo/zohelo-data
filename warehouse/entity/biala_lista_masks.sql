-- Silver: MF VAT white list (flat file) — virtual-account masks (X = account digit as
-- published, Y = digits that identify the payer inside a virtual account range).
-- Applied by the loader to ONE Bronze file; `bronze` is that file (design #875).
select
    entity,
    value as account_mask,
    cast(strptime(generation_date, '%Y%m%d') as date) as file_date,
    position as list_position,
    raw_sha256,
    source_version
from bronze
where record_kind = 'mask'
