{# WDI observations of the newest bulk archive, typed: one row per economy × indicator × year.
   Replaces the legacy pass-through table of the same name. #}
with newest as (
    select * from {{ source('world_bank_wdi', 'wdi_bulk_observations') }}
    where source_version = (select max(source_version) from {{ source('world_bank_wdi', 'wdi_bulk_observations') }})
)
select
    country_code as economy_code,
    indicator_code,
    cast(year as integer) as year,
    try_cast(value_text as double) as value,
    value_text,
    raw_sha256,
    source_version
from newest
