{{ config(alias='build_who', tags=['gold_block']) }}
{# Gold build stage: WHO data.who.int indicators of poc_who_blocks (life expectancy and healthy life expectancy at
   birth, years) for the EU27 members and World (→ WB:WLD), annual, by sex: sex_code T/M/F and breakdown_json
   {"sex": <WHO code>}, is_total for both sexes. measure_key who:<entity>:years:<indicator code>. Values as
   published (the point estimate; bounds stay in Silver); published rows only (publish_state PUBLISHED).
   Areas through poc_map_geography (section who). #}
with blocks as ({{ poc_who_blocks() }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'who' and geography_key <> '_X'
),

shaped as (
    select
        o.*,
        case upper(o.sex) when 'FEMALE' then 'F' when 'MALE' then 'M' when 'TOTAL' then 'T' when 'BOTHSEXES' then 'T'
        end as sex_code
    from {{ ref('who_observations') }} as o
)

select
    'who' as source_system,
    o.entity as block_id,
    b.block_name,
    'who:' || o.entity || ':years:' || o.indicator_code as measure_key,
    o.indicator_name as measure_name,
    o.indicator_code as measure_codes,
    'years' as unit_code,
    'years' as unit_name,
    o.geo_type || ':' || o.geo_code_m49 as geography_source_code,
    g.geography_key,
    'A' as frequency,
    make_date(o.year, 1, 1) as date_key,
    cast(o.year as varchar) as period_label,
    '{"sex": "' || o.sex || '"}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    o.sex_code,
    coalesce(o.sex_code = 'T', false) as is_total,
    o.value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from shaped as o
inner join blocks as b on o.entity = b.entity
inner join geo_map as g on o.geo_type || ':' || o.geo_code_m49 = g.source_code
where o.value is not null and o.time_type = 'YEAR' and o.publish_state = 'PUBLISHED'
