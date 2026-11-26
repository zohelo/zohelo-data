{{ config(alias='build_pzh_epimeld', tags=['gold_block']) }}
{# Gold build stage: NIZP PZH-PIB EPIMELD annual bulletins (register PLS-019), annual observations of
   silver.pzh_epimeld_cases. Block 'annual': per disease (ICD-10 codes of the bulletin's table title, part of the
   measure) two measures, registered cases ('cases') and incidence per 100,000 population ('per_100k', the
   denominator stays with the measure); frequency A only. Geography: Poland ('PL') and the voivodeships (TERYT code,
   poc_geography__pzh_epimeld). No breakdown (is_total). Values and units as published (D33); a value of a bulletin
   whose text says "wstępne" is 'provisional'. #}
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
    'pzh_epimeld:annual:' || s.unit_code || ':' || s.disease_codes as measure_key,
    case s.measure
        when 'cases' then 'Registered cases: '
        else 'Incidence per 100,000 population: '
    end || s.disease_label || ' (NIZP PZH-PIB, annual)' as measure_name,
    s.disease_codes as measure_codes,
    s.unit_code,
    case s.unit_code when 'cases' then 'cases' else 'per 100,000 population' end as unit_name,
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
    case when s.preliminary then 'provisional' else 'ok' end as observation_status_key,
    s.source_version,
    s.raw_sha256
from measures as s
left join geo as g on s.area_code = g.source_code
where s.value is not null
