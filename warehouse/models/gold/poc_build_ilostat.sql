{{ config(alias='build_ilostat', tags=['gold_block']) }}
{# Gold build stage: the ILOSTAT indicator files of poc_ilostat_blocks from silver.ilostat_observations in the
   common shape, for the EU27 members (ISO 3166 alpha-3 → poc_map_geography section ilostat, static; macro
   poc_m49_eu27). National (reported) series, per cent. measure_key ilostat:<entity>:PC[:<age band>]: the entity
   carries the indicator and the frequency, so the annual and the quarterly unemployment rate are separate
   measures (one frequency per measure); the youth/adult age bands of poc_ilostat_ages (15+, 15-24, 25+, 15-64)
   are measures, the youth NEET file has no age band. Sex is the breakdown ({"sex": <ILOSTAT code>}; sex_code
   T/M/F, is_total for SEX_T); SEX_O is not built. The survey (ILOSTAT `source`: a labour force survey, in early years
   also a census or an official estimate) is not part of the series: the ILO publishes one per area, indicator
   and period (0 periods with several, Dev trial 43290214779), and a change of survey is a break the ILO flags
   (obs_status B → break; U unreliable → low_reliability; any other flag → flagged). Values as published; a
   cell without a value is not loaded; a period that starts after the build day cannot be an observation and is
   left out (none for the EU27 today; the ILO aggregates X01 …, not built, carry periods up to 2027). #}
with blocks as ({{ poc_ilostat_blocks() }}),

ages as ({{ poc_ilostat_ages() }}),

eu27 as ({{ poc_m49_eu27() }}),

geo_map as (
    select m.*
    from {{ ref('poc_map_geography') }} as m
    inner join eu27 as e on m.geography_key = e.iso2
    where m.source_system = 'ilostat'
),

shaped as (
    select
        o.*,
        b.block_name,
        b.measure_label,
        a.age_label,
        case o.sex when 'SEX_T' then 'T' when 'SEX_M' then 'M' when 'SEX_F' then 'F' end as sex_code,
        case
            when o.freq = 'A' and regexp_matches(o.time_period, '^[0-9]{4}$')
                then make_date(cast(o.time_period as integer), 1, 1)
            when o.freq = 'Q' and regexp_matches(o.time_period, '^[0-9]{4}Q[1-4]$')
                then
                    make_date(
                        cast(left(o.time_period, 4) as integer), cast(right(o.time_period, 1) as integer) * 3 - 2, 1
                    )
        end as date_key
    from {{ ref('ilostat_observations') }} as o
    inner join blocks as b on o.entity = b.entity
    left join ages as a on o.classif1 = a.classif1
    where
        o.obs_value is not null
        and o.classif2 is null
        and (a.classif1 is not null or (o.classif1 is null and o.entity = 'eip_neet_sex_rt_a'))
)

select
    'ilostat' as source_system,
    s.entity as block_id,
    s.block_name,
    'ilostat:' || s.entity || ':PC' || coalesce(':' || s.classif1, '') as measure_key,
    s.measure_label || coalesce(', age ' || s.age_label, '') as measure_name,
    coalesce(s.classif1, s.indicator) as measure_codes,
    'PC' as unit_code,
    'percent' as unit_name,
    s.ref_area as geography_source_code,
    g.geography_key,
    s.freq as frequency,
    s.date_key,
    s.time_period as period_label,
    '{"sex": "' || s.sex || '"}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    s.sex_code,
    s.sex_code = 'T' as is_total,
    s.obs_value as value,
    case
        when s.obs_status is null then 'ok'
        when s.obs_status = 'B' then 'break'
        when s.obs_status = 'U' then 'low_reliability'
        else 'flagged'
    end as observation_status_key,
    s.source_version,
    s.raw_sha256
from shaped as s
inner join geo_map as g on s.ref_area = g.source_code
where s.sex_code is not null and s.date_key is not null and s.date_key <= current_date
