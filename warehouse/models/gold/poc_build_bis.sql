{{ config(alias='build_bis', tags=['gold_block']) }}
{# Gold build stage: the BIS series of poc_bis_measures (macros/poc_blocks.sql) from silver.bis_observations in the
   common shape. One measure per built series code, one frequency per measure (policy rates and effective
   exchange rates monthly, the property price and credit selections quarterly): measure_key
   bis:<entity>:<BIS unit code>[:<codes>], e.g. bis:cbpol:368, bis:eer:882:R.B (real, broad basket), bis:spp:771:N
   (nominal residential property prices, year-on-year change), bis:tc:770:P.A.M (credit to the private
   non-financial sector, all lenders, market value, percentage of GDP, adjusted for breaks), bis:credit_gap:770:P.A.C,
   bis:dsr:367:P. Values and units as published (D33); a cell without a value (NaN, OBS_STATUS M) is not loaded.
   Areas through poc_map_geography (section bis, static): ISO 3166 alpha-2 countries and XM euro area →
   EUROSTAT:EA; the BIS aggregates 4T, 5R, 5A, G2 and XW have no Gold area and are not built (poc_bis_areas); any
   other unknown code maps to _X and is counted by build_check. No breakdowns: every series is its own measure. #}
with measures as ({{ poc_bis_measures() }}),

blocks as ({{ poc_bis_blocks() }}),

not_built_areas as (
    select area_code
    from ({{ poc_bis_areas() }})
    where candidate_key = ''
),

obs as (
    select
        o.*,
        case o.entity
            when 'cbpol' then ''
            when 'eer'
                then
                    json_extract_string(o.dimensions_json, '$.EER_TYPE')
                    || '.' || json_extract_string(o.dimensions_json, '$.EER_BASKET')
            when 'spp'
                then
                    json_extract_string(o.dimensions_json, '$.VALUE')
                    || '.' || json_extract_string(o.dimensions_json, '$.UNIT_MEASURE')
            when 'tc'
                then
                    json_extract_string(o.dimensions_json, '$.TC_BORROWERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.TC_LENDERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.VALUATION')
                    || '.' || json_extract_string(o.dimensions_json, '$.UNIT_TYPE')
                    || '.' || json_extract_string(o.dimensions_json, '$.TC_ADJUST')
            when 'credit_gap'
                then
                    json_extract_string(o.dimensions_json, '$.TC_BORROWERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.TC_LENDERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.CG_DTYPE')
            when 'dsr' then json_extract_string(o.dimensions_json, '$.DSR_BORROWERS')
        end as series_code
    from {{ ref('bis_observations') }} as o
    where
        o.obs_value is not null
        and o.ref_area not in (select n.area_code from not_built_areas as n)
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'bis'
)

select
    'bis' as source_system,
    o.entity as block_id,
    b.block_name,
    'bis:' || o.entity || ':' || m.unit_code || case when m.measure_codes = '' then '' else ':' || m.measure_codes end
        as measure_key,
    m.measure_name,
    m.measure_codes,
    m.unit_code,
    m.unit_name,
    o.ref_area as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    o.freq as frequency,
    case
        when o.freq = 'M'
            then make_date(cast(left(o.time_period, 4) as integer), cast(right(o.time_period, 2) as integer), 1)
        when o.freq = 'Q'
            then make_date(cast(left(o.time_period, 4) as integer), cast(right(o.time_period, 1) as integer) * 3 - 2, 1)
    end as date_key,
    o.time_period as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    o.obs_value as value,
    case o.obs_status when 'B' then 'break' when 'P' then 'provisional' when 'E' then 'estimated' else 'ok' end
        as observation_status_key,
    o.source_version,
    o.raw_sha256
from obs as o
inner join measures as m on o.entity = m.entity and o.series_code = m.series_code
inner join blocks as b on o.entity = b.entity
left join geo_map as g on o.ref_area = g.source_code
where
    (o.freq = 'M' and regexp_matches(o.time_period, '^[0-9]{4}-[0-9]{2}$'))
    or (o.freq = 'Q' and regexp_matches(o.time_period, '^[0-9]{4}-Q[1-4]$'))
