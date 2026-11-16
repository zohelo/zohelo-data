{{ config(alias='build_zus_yearbook', tags=['gold_block']) }}
{# Gold build stage: ZUS social insurance statistical yearbook (register PLS-010), annual observations of
   silver.zus_yearbook_tables. One block per yearbook table, one measure_key per measure, frequency A only (stocks at
   31 December, date_key = 1 January of the year as for every annual fact):
     payers_voiv   active contribution payers (accounts), unit payers
     insured_voiv  persons insured in old-age and disability pension insurance, by the payer's seat, unit persons
   The area is the voivodeship name or 'polska', mapped in poc_map_geography (TERYT names). Cells with a ZUS sign
   instead of a number (cell_status other than 'ok') carry no value and are not loaded. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'zus_yearbook'
)

select
    'zus_yearbook' as source_system,
    s.table_code as block_id,
    case s.table_code
        when 'payers_voiv' then 'ZUS yearbook: contribution payers by voivodeship, 31 December'
        when 'insured_voiv' then 'ZUS yearbook: insured persons by voivodeship, 31 December'
    end as block_name,
    'zus_yearbook:' || s.table_code || ':' || s.unit_code || ':' || s.measure_code as measure_key,
    case s.measure_code
        when 'payers' then 'Contribution payers (active accounts) in ZUS, 31 December'
        when 'insured' then 'Persons insured in ZUS pension insurance, by payer seat, 31 December'
    end as measure_name,
    s.measure_code as measure_codes,
    s.unit_code,
    case s.unit_code
        when 'payers' then 'contribution payers'
        when 'persons' then 'persons'
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
from {{ ref('zus_yearbook_tables') }} as s
left join geo as g on s.area_name = g.source_code
where s.cell_status = 'ok'
