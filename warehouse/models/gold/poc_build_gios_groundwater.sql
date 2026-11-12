{{ config(alias='build_gios_groundwater', tags=['gold_block']) }}
{# Gold build stage (#2870): GIOŚ groundwater quality monitoring (register PL-ENV-012) as yearly statistical
   observations for Poland, counted from silver.gios_groundwater_quality_classes (one row per monitoring point and
   year with its final quality class). One block, 'quality_classes', two measures per monitoring network (the
   diagnostic network, about 1,000-1,400 points every three years or so, and the operational network of points at
   risk, about 300-400 points in the other years, are different samples and never one series; 2004-2006 name no
   network, not_stated):
     gios_groundwater:quality_classes:points:<network>      monitoring points by final class, breakdown
                                                            quality_class I-V and _T (all points)
     gios_groundwater:quality_classes:pct:I-III:<network>   share of points in classes I-III (good chemical
                                                            status), % of the points classified that year
   The workbooks publish no TERYT code (voivodeship by name only), so the geography is Poland. #}
with points as (
    select
        year,
        monitoring_network,
        final_class,
        source_version,
        raw_sha256
    from {{ ref('gios_groundwater_quality_classes') }}
),

by_class as (
    select
        year,
        monitoring_network,
        final_class as quality_class,
        count(*) as points
    from points
    group by year, monitoring_network, final_class
    union all
    select
        year,
        monitoring_network,
        '_T' as quality_class,
        count(*) as points
    from points
    group by year, monitoring_network
),

good as (
    select
        year,
        monitoring_network,
        100.0 * count(*) filter (where final_class in ('I', 'II', 'III')) / count(*) as share
    from points
    group by year, monitoring_network
),

versions as (  -- the workbook of each year (one classes workbook a year)
    select
        year,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from points
    group by year
)

select
    'gios_groundwater' as source_system,
    'quality_classes' as block_id,
    'GIOŚ groundwater quality classes of monitoring points' as block_name,
    'gios_groundwater:quality_classes:points:' || b.monitoring_network as measure_key,
    'Groundwater monitoring points by final quality class, ' || replace(b.monitoring_network, '_', ' ')
    || ' monitoring' as measure_name,
    b.monitoring_network as measure_codes,
    'points' as unit_code,
    'monitoring points' as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    'A' as frequency,
    make_date(b.year, 1, 1) as date_key,
    cast(b.year as varchar) as period_label,
    cast(json_object('quality_class', b.quality_class) as varchar) as breakdown_json,
    cast(json_object('quality_class', case b.quality_class when '_T' then 'ogółem' else 'klasa ' || b.quality_class end)
        as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    b.quality_class = '_T' as is_total,
    cast(b.points as double) as value,
    'ok' as observation_status_key,
    v.source_version,
    v.raw_sha256
from by_class as b
inner join versions as v on b.year = v.year
union all
select
    'gios_groundwater' as source_system,
    'quality_classes' as block_id,
    'GIOŚ groundwater quality classes of monitoring points' as block_name,
    'gios_groundwater:quality_classes:pct:I-III:' || g.monitoring_network as measure_key,
    'Share of groundwater monitoring points in quality classes I-III (good chemical status), '
    || replace(g.monitoring_network, '_', ' ') || ' monitoring' as measure_name,
    'I-III,' || g.monitoring_network as measure_codes,
    'pct' as unit_code,
    '% of monitoring points' as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    'A' as frequency,
    make_date(g.year, 1, 1) as date_key,
    cast(g.year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    g.share as value,
    'ok' as observation_status_key,
    v.source_version,
    v.raw_sha256
from good as g
inner join versions as v on g.year = v.year
