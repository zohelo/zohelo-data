{{ config(alias='build_gus_nsp2021', tags=['gold_block']) }}
{# Gold build stage: GUS NSP 2021 census final results (reference date 31 March) (register PLS-005), annual
   observations of silver.gus_nsp2021_tables. Block 'census_2021': one measure_key per measure (frequency A only; the
   census year is the period, the reference date is in the measure name); the area is the voivodeship name or
   'polska', mapped in poc_map_geography (TERYT names). Rows without a value are not loaded. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gus_nsp2021'
)

select
    'gus_nsp2021' as source_system,
    'census_2021' as block_id,
    'GUS NSP 2021 census final results (reference date 31 March)' as block_name,
    'gus_nsp2021:census_2021:' || s.unit_code || ':' || s.measure_code as measure_key,
    case s.measure_code
        when 'population' then 'Population, census (31 March 2021; 31 March 2011) (persons)'
        when 'population_urban' then 'Urban population, census 2021 (persons)'
        when 'population_rural' then 'Rural population, census 2021 (persons)'
        when 'urban_share' then 'Urban population in % of total, census 2021'
        when 'activity_rate' then 'Activity rate of persons aged 15+, census 2021 (%)'
        when 'employment_rate' then 'Employment rate of persons aged 15+, census 2021 (%)'
        when 'unemployment_rate' then 'Unemployment rate, census 2021 (%)'
    end as measure_name,
    s.measure_code as measure_codes,
    s.unit_code,
    case s.unit_code
        when 'persons' then 'persons'
        when 'pct_population' then '% of population'
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
    case s.cell_status
        when 'low_reliability' then 'low_reliability'
        when 'below_half_unit' then 'flagged'
        else 'ok'
    end as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('gus_nsp2021_tables') }} as s
left join geo as g on s.area_name = g.source_code
where s.value is not null
