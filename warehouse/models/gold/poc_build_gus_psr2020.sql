{{ config(alias='build_gus_psr2020', tags=['gold_block']) }}
{# Gold build stage: GUS PSR 2020 agricultural census (holdings by voivodeship) (register PLS-006), annual
   observations of silver.gus_psr2020_tables. Block 'agricultural_census_2020': one measure_key per measure
   (frequency A only; the census year is the period, the reference date is in the measure name); the area is the
   voivodeship name or 'polska', mapped in poc_map_geography (TERYT names). Rows without a value are not loaded. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gus_psr2020'
)

select
    'gus_psr2020' as source_system,
    'agricultural_census_2020' as block_id,
    'GUS PSR 2020 agricultural census (holdings by voivodeship)' as block_name,
    'gus_psr2020:agricultural_census_2020:' || s.unit_code || ':' || s.measure_code as measure_key,
    case s.measure_code
        when 'farms' then 'Agricultural holdings, census (2020; 2010)'
        when 'agricultural_land' then 'Agricultural land of holdings, census 2020 (ha)'
        when 'average_agricultural_land' then 'Average agricultural land per holding, census 2020 (ha)'
        when 'cattle' then 'Cattle in holdings, census 2020 (heads)'
        when 'pigs' then 'Pigs in holdings, census 2020 (heads)'
    end as measure_name,
    s.measure_code as measure_codes,
    s.unit_code,
    case s.unit_code
        when 'farms' then 'holdings'
        when 'ha' then 'ha'
        when 'heads' then 'heads'
        when 'ha_per_holding' then 'ha per holding'
    end as unit_name,
    s.area_name as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(s.period_year, 1, 1) as date_key,
    cast(s.period_year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    s.value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('gus_psr2020_tables') }} as s
left join geo as g on s.area_name = g.source_code
where s.value is not null
