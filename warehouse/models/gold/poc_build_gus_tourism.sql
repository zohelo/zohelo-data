{{ config(alias='build_gus_tourism', tags=['gold_block']) }}
{# Gold build stage: GUS tourism tables: tourist accommodation (annual) (register PLS-035), annual observations of
   silver.gus_tourism_tables. Block 'accommodation': one measure_key per measure (frequency A only); the area is the
   voivodeship name or 'polska', mapped in poc_map_geography (TERYT names). Rows without a value (GUS '.', 'x') are
   not loaded; survey estimates marked 'v' carry observation status low_reliability. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gus_tourism'
)

select
    'gus_tourism' as source_system,
    'accommodation' as block_id,
    'GUS tourism tables: tourist accommodation (annual)' as block_name,
    'gus_tourism:accommodation:' || s.unit_code || ':' || s.measure_code as measure_key,
    case s.measure_code
        when 'establishments' then 'Tourist accommodation establishments, 31 July (facilities)'
        when 'bed_places' then 'Bed places in tourist accommodation establishments, 31 July'
        when 'tourists' then 'Tourists accommodated in tourist accommodation establishments (persons)'
        when 'overnight_stays' then 'Overnight stays in tourist accommodation establishments'
    end as measure_name,
    s.measure_code as measure_codes,
    s.unit_code,
    case s.unit_code
        when 'persons' then 'persons'
        when 'facilities' then 'facilities'
        when 'bed_places' then 'bed places'
        when 'overnight_stays' then 'overnight stays'
        when 'pct_population' then '% of persons aged 15+'
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
    case s.cell_status when 'low_reliability' then 'low_reliability' else 'ok' end as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('gus_tourism_tables') }} as s
left join geo as g on s.area_name = g.source_code
where s.value is not null
