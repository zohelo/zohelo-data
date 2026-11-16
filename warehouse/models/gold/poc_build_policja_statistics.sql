{{ config(alias='build_policja_statistics', tags=['gold_block']) }}
{# Gold build stage: Polish Police statistics portal (register PLG-033), annual observations of
   silver.policja_statistics_offences. Blocks = category workbooks ('ogolem' all offences, 'wybrane_7' seven selected
   categories); one measure_key per category x measure (frequency A only): offences recorded, offences detected
   (unit offences) and the detection rate (unit pct, as published). Areas: Poland and the 16 voivodeships, mapped in
   poc_map_geography (macros/sources/policja_statistics.sql) by the printed name without a trailing note in brackets
   ('woj. mazowieckie (KWP z/s w Radomiu i KSP Warszawa)' -> 'woj. mazowieckie'; geography_source_code keeps the
   printed name). The two police areas that split Mazowieckie ('teren działania KSP Warszawa', 'teren działania KWP
   z/s w Radomiu') stay in Silver only. Cells without a value ('n.d.', empty) are not loaded. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'policja_statistics'
)

select
    'policja_statistics' as source_system,
    s.category_code as block_id,
    case s.category_code
        when 'ogolem' then 'Police statistics: all offences by voivodeship (annual)'
        when 'wybrane_7' then 'Police statistics: seven selected offence categories by voivodeship (annual)'
    end as block_name,
    'policja_statistics:' || s.category_code || ':' || s.unit_code || ':' || s.measure_code as measure_key,
    case s.category_code
        when 'ogolem' then 'All offences'
        when 'wybrane_7' then 'Seven selected offence categories'
    end
    || ': '
    || case s.measure_code
        when 'recorded' then 'offences recorded (przestępstwa stwierdzone)'
        when 'detected' then 'offences detected (przestępstwa wykryte)'
        when 'detection_rate' then 'detection rate, % (% wykrycia)'
    end as measure_name,
    s.measure_code as measure_codes,
    s.unit_code,
    case s.unit_code
        when 'offences' then 'offences'
        when 'pct' then '%'
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
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('policja_statistics_offences') }} as s
left join geo as g on regexp_replace(s.area_name, '\s*\(.*\)$', '') = g.source_code
where
    s.value is not null
    and (s.area_name = 'Polska' or s.area_name like 'woj. %')
