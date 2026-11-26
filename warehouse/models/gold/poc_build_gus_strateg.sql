{{ config(alias='build_gus_strateg', tags=['gold_block']) }}
{# Gold build stage: GUS STRATEG (register PLS-003), block 'sor', the observed values of the indicators of the Strategy
   for Responsible Development from silver.gus_strateg_observations. One measure per SOR indicator dimension:
   measure_key gus_strateg:sor:<unit as published>:<indicator_id>.<dimension_id>, e.g. gus_strateg:sor:%:14.5 (R&D
   expenditure in relation to GDP); annual only, so one frequency per measure. Values and units as published (D33);
   values that are not numbers and the zero-filled voivodeship years of Silver (value null) are not loaded. The strategy's base, intermediate and target
   values are not observations and stay in silver.gus_strateg_indicators. Areas through poc_map_geography (section
   gus_strateg, static): level 0 keeps the EU countries and aggregates (codes ending in ten zeros), level 2 the
   voivodeships; Poland is taken from level 0 only, so no area comes from two levels. 030000000000 (EU-28) has no Gold
   area and is not built; any other code maps to _X and is counted by build_check. A year after the build day cannot
   be an observation and is left out. Overlap with GUS BDL and Eurostat is expected: kept as this source's series. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gus_strateg'
),

obs as (
    select *
    from {{ ref('gus_strateg_observations') }}
    where
        value is not null
        and territory_code <> '030000000000'
        and (
            (level_id = '0' and territory_code like '__0000000000')
            or (level_id = '2' and territory_code like '10__00000000' and territory_code <> '100000000000')
        )
)

select
    'gus_strateg' as source_system,
    'sor' as block_id,
    'GUS STRATEG: indicators of the Strategy for Responsible Development (SOR)' as block_name,
    'gus_strateg:sor:' || o.unit_code || ':' || o.indicator_id || '.' || o.dimension_id as measure_key,
    coalesce(i.indicator_name_en, i.indicator_name_pl) as measure_name,
    o.indicator_id || '.' || o.dimension_id as measure_codes,
    o.unit_code,
    coalesce(i.unit_en, o.unit_code) as unit_name,
    o.territory_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(o.period_year, 1, 1) as date_key,
    cast(o.period_year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    o.value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from obs as o
inner join {{ ref('gus_strateg_indicators') }} as i
    on o.indicator_id = i.indicator_id and o.dimension_id = i.dimension_id
left join geo as g on o.territory_code = g.source_code
where make_date(o.period_year, 1, 1) <= current_date
