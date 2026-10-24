-- Silver: MF VAT white list (flat file) — one row per published SHA-512 hash of an active or
-- exempt VAT taxpayer's (date + NIP + bank account). A NIP+account pair is checked by hashing
-- generation date + NIP + account `transformations` times (MF scheme) and looking it up here.
-- Applied by the loader to ONE Bronze file; `bronze` is that file (design #875).
select
    entity,
    value as hash_sha512,
    record_kind as taxpayer_status,
    cast(strptime(generation_date, '%Y%m%d') as date) as file_date,
    cast(transformations as integer) as transformations,
    position as list_position,
    raw_sha256,
    source_version
from bronze
where record_kind in ('active', 'exempt')
