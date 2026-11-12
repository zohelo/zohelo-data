{{ config(alias='build_gus_culture_participation', tags=['gold_block']) }}
{# Gold build stage: GUS participation in culture survey (persons aged 15+) (register PLS-028), annual observations
   of silver.gus_culture_participation_tables. Block 'participation': one measure_key per measure (frequency A only);
   the area is the voivodeship name or 'polska', mapped in poc_map_geography (TERYT names). Rows without a value (GUS
   '.', 'x') are not loaded; survey estimates marked 'v' carry observation status low_reliability. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gus_culture_participation'
)

select
    'gus_culture_participation' as source_system,
    'participation' as block_id,
    'GUS participation in culture survey (persons aged 15+)' as block_name,
    'gus_culture_participation:participation:' || s.unit_code || ':' || s.measure_code as measure_key,
    case s.measure_code
        when 'museum_visiting' then 'Persons aged 15+ who visited a museum in Poland in the past 12 months (%)'
        when 'library_use' then 'Persons aged 15+ who used a library or reading room in the past 12 months (%)'
        when 'theatre_attendance' then 'Persons aged 15+ who went to the theatre in the past 12 months (%)'
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
    cast(to_json(map(['sex'], [s.group_code])) as varchar) as breakdown_json,
    cast(
        to_json(map(
            ['sex'], [case s.group_code when 'T' then 'ogółem' when 'F' then 'kobiety' else 'mężczyźni' end]
        )) as varchar
    ) as breakdown_label_json,
    s.group_code as sex_code,
    s.group_code = 'T' as is_total,
    s.value,
    case s.cell_status when 'low_reliability' then 'low_reliability' else 'ok' end as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('gus_culture_participation_tables') }} as s
left join geo as g on s.area_name = g.source_code
where s.value is not null
