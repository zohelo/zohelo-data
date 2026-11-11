{# FAOSTAT domain observations (bulk "All Data (Normalized)" zips, one entity per domain: emissions_totals = GT,
   land_use = RL): one row per area, item, element, year and source of the newest Landing copy of each entity,
   typed. Codes as published: area_code is FAO's area code (countries and FAO aggregates such as 5000 World),
   area_code_m49 the UN M49 code without FAO's leading apostrophe; item and element codes with their labels;
   source_code/source only in domains that have them (GT: e.g. 3050 FAO TIER 1), else null. Value in the
   published unit (kt, 1000 ha, %); flag as published (A official, E estimated, I imputed, ...). Rows of
   superseded copies are not kept; a duplicated row fails the unique test instead of being dropped. #}
with newest as (
    select *
    from {{ source('faostat', 'faostat_observations') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    entity,
    try_cast(area_code as integer) as area_code,
    nullif(ltrim(area_code_m49, ''''), '') as area_code_m49,
    area,
    item_code,
    item,
    element_code,
    element,
    try_cast(year as integer) as year,
    year_code,
    coalesce(source_code, '_Z') as source_code,
    source,
    unit,
    try_cast(nullif(value, '') as double) as value,
    nullif(flag, '') as flag,
    nullif(note, '') as note,
    raw_sha256,
    source_version,
    ingested_at,
    source_row_number
from newest
