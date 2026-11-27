{{ config(alias='build_pzh_epimeld', tags=['gold_block']) }}
{# Gold build stage: NIZP PZH-PIB EPIMELD annual bulletins (register PLS-019), annual observations of
   silver.pzh_epimeld_cases. Block 'annual': per disease (disease_key: the bulletin's disease line, part of the
   measure) two measures, registered cases ('cases') and incidence ('per_100k' per 100,000 population,
   'per_100k_live_births', 'per_100k_children_under_2' or 'per_100k_children_0_14': the denominator stays with the
   measure); frequency A only.
   Geography: Poland ('PL') and the voivodeships (TERYT code, poc_geography__pzh_epimeld). No breakdown (is_total).
   Values and units as published (D33). Status: 'provisional' for a bulletin whose text says "wstępne", 'flagged'
   for a printed row whose quarters do not add up to its year (a misprint kept as published), else 'ok'. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'pzh_epimeld'
),

measures as (
    select
        s.*,
        m.measure,
        case m.measure when 'cases' then cast(s.cases as double) else s.incidence end as value,
        case m.measure when 'cases' then 'cases' else s.incidence_unit_code end as unit_code
    from {{ ref('pzh_epimeld_cases') }} as s
    cross join (values ('cases'), ('incidence')) as m (measure)
)

select
    'pzh_epimeld' as source_system,
    'annual' as block_id,
    'NIZP PZH-PIB infectious diseases: cases and incidence by disease (annual bulletins)' as block_name,
    'pzh_epimeld:annual:' || s.unit_code || ':' || s.disease_key as measure_key,
    case s.unit_code when 'cases' then 'Registered cases: ' else 'Incidence ' || u.unit_name || ': ' end
    || s.disease_label || ' (NIZP PZH-PIB, annual)' as measure_name,
    coalesce(s.disease_codes, s.disease_key) as measure_codes,
    s.unit_code,
    u.unit_name,
    s.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    s.period_date as date_key,
    cast(s.period_year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    s.value,
    case
        when s.preliminary then 'provisional'
        when not s.quarters_match then 'flagged'
        else 'ok'
    end as observation_status_key,
    s.source_version,
    s.raw_sha256
from measures as s
left join geo as g on s.area_code = g.source_code
left join (
    values
    ('cases', 'cases'),
    ('per_100k', 'per 100,000 population'),
    ('per_100k_live_births', 'per 100,000 live births'),
    ('per_100k_children_under_2', 'per 100,000 children under 2'),
    ('per_100k_children_0_14', 'per 100,000 children aged 0-14')
) as u (unit_code, unit_name) on s.unit_code = u.unit_code
where s.value is not null
