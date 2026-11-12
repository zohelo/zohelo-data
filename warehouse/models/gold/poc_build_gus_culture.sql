{{ config(alias='build_gus_culture', tags=['gold_block']) }}
{# Gold build stage: GUS culture and national heritage tables (annual) (register PLS-027), annual observations of
   silver.gus_culture_tables. Block 'culture_institutions': one measure_key per measure (frequency A only); the area
   is the voivodeship name or 'polska', mapped in poc_map_geography (TERYT names). Rows without a value (GUS '.',
   'x') are not loaded; survey estimates marked 'v' carry observation status low_reliability. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gus_culture'
)

select
    'gus_culture' as source_system,
    'culture_institutions' as block_id,
    'GUS culture and national heritage tables (annual)' as block_name,
    'gus_culture:culture_institutions:' || s.unit_code || ':' || s.measure_code as measure_key,
    case s.measure_code
        when 'museum_visitors' then 'Museum and museum exhibition visitors (persons)'
        when 'cinema_admissions' then 'Cinema admissions (persons)'
        when 'cinemas' then 'Cinemas, 31 December (facilities)'
        when 'library_users' then 'Public library users (borrowers) (persons)'
        when 'public_libraries' then 'Public libraries and branches, 31 December (facilities)'
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
from {{ ref('gus_culture_tables') }} as s
left join geo as g on s.area_name = g.source_code
where s.value is not null
