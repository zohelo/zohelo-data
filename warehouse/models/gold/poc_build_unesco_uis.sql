{{ config(alias='build_unesco_uis', tags=['gold_block']) }}
{# Gold build stage: UNESCO UIS blocks of poc_unesco_uis_blocks (GERD as % of GDP; tertiary attainment 25+, %) for
   the EU27 members (ISO alpha-3 → poc_map_geography section unesco_uis), annual. Tertiary attainment is one measure
   with a sex breakdown: EA.5T8.AG25T99 = both sexes (sex T, is_total), the .F and .M variants = F and M,
   breakdown_json {"sex": ...}. measure_key unesco_uis:<entity>:<unit>:<base indicator>. Qualifiers UIS_EST and
   NAT_EST (UIS or national estimate) → observation status estimated, else ok. Values as published. #}
with blocks as ({{ poc_unesco_uis_blocks() }}),

eu27 as ({{ poc_m49_eu27() }}),

geo_map as (
    select m.*
    from {{ ref('poc_map_geography') }} as m
    inner join eu27 as e on m.geography_key = e.iso2
    where m.source_system = 'unesco_uis'
),

shaped as (
    select
        o.*,
        b.block_name,
        b.unit_code,
        b.indicator_id as base_indicator,
        case
            when o.indicator_id = b.indicator_id then 'T'
            when o.indicator_id = b.indicator_id || '.F' then 'F'
            when o.indicator_id = b.indicator_id || '.M' then 'M'
        end as sex_code
    from {{ ref('unesco_uis_observations') }} as o
    inner join blocks as b on o.entity = b.entity
)

select
    'unesco_uis' as source_system,
    s.entity as block_id,
    s.block_name,
    'unesco_uis:' || s.entity || ':' || s.unit_code || ':' || s.base_indicator as measure_key,
    s.block_name as measure_name,
    s.base_indicator as measure_codes,
    s.unit_code,
    case s.unit_code when 'pct_gdp' then '% of GDP' else '%' end as unit_name,
    s.geo_unit as geography_source_code,
    g.geography_key,
    'A' as frequency,
    make_date(s.year, 1, 1) as date_key,
    cast(s.year as varchar) as period_label,
    case when s.entity = 'tertiary_attainment' then '{"sex": "' || s.sex_code || '"}' else '{}' end as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    case when s.entity = 'tertiary_attainment' then s.sex_code end as sex_code,
    s.sex_code = 'T' as is_total,
    s.value,
    case when s.qualifier in ('UIS_EST', 'NAT_EST') then 'estimated' else 'ok' end as observation_status_key,
    s.source_version,
    s.raw_sha256
from shaped as s
inner join geo_map as g on s.geo_unit = g.source_code
where s.value is not null and s.sex_code is not null
