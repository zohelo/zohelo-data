-- GLEIF reporting exceptions: why an LEI reports no direct or ultimate parent, newest golden
-- copy; up to five reasons and references as published.
-- Applied by the loader to ONE Bronze file; `bronze` is that file (design #875).
select
    entity,
    lei,
    exception_category,
    nullif(exception_reason_1, '') as reason_1,
    nullif(exception_reason_2, '') as reason_2,
    nullif(exception_reason_3, '') as reason_3,
    nullif(exception_reason_4, '') as reason_4,
    nullif(exception_reason_5, '') as reason_5,
    nullif(exception_reference_1, '') as reference_1,
    nullif(exception_reference_2, '') as reference_2,
    nullif(exception_reference_3, '') as reference_3,
    nullif(exception_reference_4, '') as reference_4,
    nullif(exception_reference_5, '') as reference_5,
    source_version,
    raw_sha256
from bronze
