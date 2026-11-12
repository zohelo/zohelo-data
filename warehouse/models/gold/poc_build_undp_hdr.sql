{{ config(alias='build_undp_hdr', tags=['gold_block']) }}
{# Gold build stage: the UNDP Human Development Report measures of poc_undp_hdr_measures from
   silver.undp_hdr_indices (newest report, complete time series) in the common shape, annual, for the EU27
   members (ISO 3166 alpha-3 → poc_map_geography section undp_hdr, static; macro poc_m49_eu27) and World
   (ZZK.WORLD → WB:WLD); the HDR development groups and UNDP regions (ZZA … ZZJ) have no Gold area and are not
   built. measure_key undp_hdr:composite_indices:<unit>:<measure>: hdi, le (life expectancy at birth), eys, mys
   (expected and mean years of schooling) and gnipc (GNI per capita, 2021 PPP $) with a sex breakdown
   ({"sex": "T" | "F" | "M"} from the source's total, _f and _m columns; is_total for the total); ihdi, gii and gdi
   without one. Values as published; the file has no status flags (ok). A new report recalculates the whole
   series, and Gold holds the newest report only. #}
with measures as ({{ poc_undp_hdr_measures() }}),

eu27 as ({{ poc_m49_eu27() }}),

geo_map as (
    select m.*
    from {{ ref('poc_map_geography') }} as m
    left join eu27 as e on m.geography_key = e.iso2
    where m.source_system = 'undp_hdr' and (e.iso2 is not null or m.geography_key = 'WB:WLD')
)

select
    'undp_hdr' as source_system,
    'composite_indices' as block_id,
    'UNDP Human Development Report composite indices, complete time series' as block_name,
    'undp_hdr:composite_indices:' || m.unit_code || ':' || m.measure as measure_key,
    m.measure_name,
    m.measure as measure_codes,
    m.unit_code,
    m.unit_name,
    o.iso3 as geography_source_code,
    g.geography_key,
    'A' as frequency,
    make_date(o.year, 1, 1) as date_key,
    cast(o.year as varchar) as period_label,
    case when m.sex_code = '' then '{}' else '{"sex": "' || m.sex_code || '"}' end as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    nullif(m.sex_code, '') as sex_code,
    m.sex_code in ('', 'T') as is_total,
    o.value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from {{ ref('undp_hdr_indices') }} as o
inner join measures as m on o.indicator = m.indicator
inner join geo_map as g on o.iso3 = g.source_code
where o.value is not null and o.year is not null
