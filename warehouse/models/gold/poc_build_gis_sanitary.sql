{{ config(alias='build_gis_sanitary', tags=['gold_block']) }}
{# Gold build stage: GIS annual report (register PLS-018), food samples examined by the State Sanitary Inspection
   laboratories and the per cent disqualified, Poland, annual, from silver.gis_sanitary_food_samples. Block
   'food_samples'; one frequency (A), date_key = 1 January of the reference year. Each year comes from its own
   report (is_report_year: the edition about that year, Stan sanitarny kraju 2012-2023, Raport z działań PIS 2024
   and 2025), one row per year and origin by construction; the reprints in later editions stay in Silver (they
   agree except the 2020 total share, 3,09 in the 2020 edition and in its components, 3,2 in the reprints).
   Measures, as printed (no share is calculated): examined_total / examined_domestic (samples) and
   disqualified_share_total / disqualified_share_domestic (per cent). The table has no suppression or missing
   marker. #}
with s as (
    select * from {{ ref('gis_sanitary_food_samples') }}
    where is_report_year
),

geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gis_sanitary'
),

observations as (
    select
        reference_year,
        'examined_' || origin_code as measure_code,
        'samples' as unit_code,
        cast(samples_examined as double) as value,
        source_version,
        raw_sha256
    from s
    union all
    select
        reference_year,
        'disqualified_share_' || origin_code as measure_code,
        'pct' as unit_code,
        cast(disqualified_pct as double) as value,
        source_version,
        raw_sha256
    from s
)

select
    'gis_sanitary' as source_system,
    'food_samples' as block_id,
    'GIS food samples examined and disqualified (annual)' as block_name,
    'gis_sanitary:food_samples:' || o.unit_code || ':' || o.measure_code as measure_key,
    case o.measure_code
        when 'examined_total' then 'Food samples examined by the State Sanitary Inspection, all foodstuffs'
        when 'examined_domestic' then 'Food samples examined by the State Sanitary Inspection, domestic foodstuffs'
        when 'disqualified_share_total'
            then 'Food samples not meeting health-quality requirements, % of samples examined, all foodstuffs'
        when 'disqualified_share_domestic'
            then 'Food samples not meeting health-quality requirements, % of samples examined, domestic foodstuffs'
    end as measure_name,
    o.measure_code as measure_codes,
    o.unit_code,
    case o.unit_code when 'samples' then 'samples' when 'pct' then 'per cent' end as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(o.reference_year, 1, 1) as date_key,
    cast(o.reference_year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    o.value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from observations as o
left join geo as g on g.source_code = 'PL'
