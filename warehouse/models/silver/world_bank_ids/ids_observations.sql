{# IDS observations of the newest bulk archive, typed: one row per debtor economy × counterpart area ×
   series × year (1970 onwards, with projections of debt service in later years). Codes are trimmed (the
   archive pads some with spaces). Debtors are low- and middle-income economies and their groups only:
   no Poland, no EU member state (Dev probe 43084359365, 11 Nov 2026). #}
with newest as (
    select * from {{ source('world_bank_ids', 'ids_observations') }}
    where
        source_version
        = (select max(latest.source_version) from {{ source('world_bank_ids', 'ids_observations') }} as latest)
)

select
    trim(country_code) as economy_code,
    country_name as economy_name,
    trim(counterpart_code) as counterpart_code,
    counterpart_name,
    trim(series_code) as series_code,
    series_name,
    cast(year as integer) as year,
    try_cast(value_text as double) as value,
    value_text,
    raw_sha256,
    source_version
from newest
