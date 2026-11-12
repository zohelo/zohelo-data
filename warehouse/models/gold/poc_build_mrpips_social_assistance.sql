{{ config(alias='build_mrpips_social_assistance', tags=['gold_block']) }}
{# Gold build stage: MRPiPS-03 social assistance report (register PLS-031), annual observations of
   silver.mrpips_social_assistance_tables. Block 'social_assistance': one measure_key per measure (frequency A only);
   the area is the voivodeship name or 'polska', mapped in poc_map_geography (TERYT names). Rows without a value are
   not loaded. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'mrpips_social_assistance'
)

select
    'mrpips_social_assistance' as source_system,
    'social_assistance' as block_id,
    'MRPiPS-03 social assistance (benefits granted, annual)' as block_name,
    'mrpips_social_assistance:social_assistance:' || s.unit_code || ':' || s.measure_code as measure_key,
    case s.measure_code
        when 'persons_granted' then 'Persons granted social assistance benefits by decision (MRPiPS-03)'
        when 'families' then 'Families granted social assistance benefits (MRPiPS-03)'
        when 'persons_in_families' then 'Persons in families granted social assistance benefits (MRPiPS-03)'
    end as measure_name,
    s.measure_code as measure_codes,
    s.unit_code,
    case s.unit_code when 'persons' then 'persons' when 'families' then 'families' end as unit_name,
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
from {{ ref('mrpips_social_assistance_tables') }} as s
left join geo as g on s.area_name = g.source_code
where s.value is not null
