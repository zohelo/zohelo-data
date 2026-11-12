{# UNESCO UIS Data API selections (entity gerd_gdp = EXPGDP.TOT; tertiary_attainment = EA.5T8.AG25T99 with its
   female .F and male .M variants): one row per indicator, geographic unit and year of the newest Landing copy of
   each entity, typed. geo_unit as published: ISO 3166-1 alpha-3 for countries, prefixed codes for regions
   (e.g. 'SDG: ...', 'WB: ...'); value in per cent; magnitude and qualifier (e.g. UIS_EST, an UIS estimate) as
   published. A duplicated record fails the unique test. #}
with newest as (
    select *
    from {{ source('unesco_uis', 'unesco_uis_observations') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    entity,
    indicator_id,
    geo_unit,
    length(geo_unit) = 3 and upper(geo_unit) = geo_unit as is_country_code,
    try_cast(year as integer) as year,
    try_cast(nullif(value, '') as double) as value,
    nullif(magnitude, '') as magnitude,
    nullif(qualifier, '') as qualifier,
    raw_sha256,
    source_version,
    ingested_at,
    source_row_number
from newest
