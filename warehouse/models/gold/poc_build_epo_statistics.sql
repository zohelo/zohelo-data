{{ config(alias='build_epo_statistics', tags=['gold_block']) }}
{# Gold build stage: EPO patent statistics (register EX-004), annual counts of silver.epo_statistics_counts.
   Blocks: by_country (European patent applications, granted European patents; per country of residence of the
   first-named applicant and the world 'Total'), by_field (the 35 fields of technology and 'Unclassified', all origins =
   the world), by_field_country (per field and country named in the year sheets) and regions (Poland's applications by
   NUTS 2 region). measure_key = epo_statistics:<block>:<unit>:<indicator>, frequency A only; the field is the
   breakdown {"technology_field": "<field as published>"}. Units as published: applications, patents (granted).
   Editions overlap in years: the newest edition holding a series' year wins (rank by edition, the workbooks' version;
   Silver keeps every edition). Not built (Silver only): 'Not classified' countries, 'Other countries' and 'Total' of
   the field x country sheets (the world by field comes from by_field), the regions' 'Total' (= Poland in by_country)
   and 'Unclassified'. Areas mapped in poc_map_geography (macros/sources/epo_statistics.sql); cells without a value are
   not loaded. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'epo_statistics'
),

s as (
    select
        c.*,
        case
            when c.table_code like '%_field_country' then 'by_field_country'
            when c.table_code like '%_by_field' then 'by_field'
            when c.table_code like '%_by_country' then 'by_country'
            when c.table_code = 'poland_regions' then 'regions'
        end as block_id,
        coalesce(g.geography_key, '_X') as geography_key
    from {{ ref('epo_statistics_counts') }} as c
    left join geo as g on c.area_label = g.source_code
    where
        c.value is not null
        and not (c.table_code like '%_by_country' and c.area_label = 'Not classified')
        and not (c.table_code like '%_field_country' and c.area_label in ('Other countries', 'Total'))
        and not (c.table_code = 'poland_regions' and c.area_label in ('Total', 'Unclassified'))
),

newest as (
    select *
    from s
    qualify rank() over (
        partition by
            block_id, indicator, case when geography_key = '_X' then area_label else geography_key end,
            coalesce(field_name, '_T'), period_year
        order by edition desc
    ) = 1
)

select
    'epo_statistics' as source_system,
    n.block_id,
    case n.block_id
        when 'by_country' then 'EPO European patent applications and grants by country of residence of the applicant'
        when 'by_field' then 'EPO European patent applications and grants by field of technology'
        when 'by_field_country' then 'EPO European patent applications and grants by field of technology and country'
        when 'regions' then 'EPO European patent applications by NUTS 2 region of Poland'
    end as block_name,
    'epo_statistics:' || n.block_id || ':' || n.unit_code || ':' || n.indicator as measure_key,
    case n.indicator
        when 'applications' then 'European patent applications (direct and Euro-PCT regional phase)'
        when 'grants' then 'European patents granted'
    end
    || case n.block_id
        when 'by_country' then ', by country of residence of the first-named applicant'
        when 'by_field' then ', by field of technology, all origins'
        when 'by_field_country' then ', by field of technology and country of residence of the first-named applicant'
        when 'regions' then ', by NUTS 2 region of residence of the first-named applicant'
    end as measure_name,
    n.indicator as measure_codes,
    n.unit_code,
    case n.unit_code
        when 'applications' then 'applications'
        when 'patents' then 'patents granted'
    end as unit_name,
    n.area_label as geography_source_code,
    n.geography_key,
    'A' as frequency,
    make_date(n.period_year, 1, 1) as date_key,
    cast(n.period_year as varchar) as period_label,
    case
        when n.field_name is null then '{}'
        else cast(json_object('technology_field', n.field_name) as varchar)
    end as breakdown_json,
    case
        when n.field_name is not null
            then cast(json_object('technology_field', coalesce(n.sector_name || ': ', '') || n.field_name) as varchar)
    end as breakdown_label_json,
    cast(null as varchar) as sex_code,
    n.field_name is null as is_total,
    n.value,
    'ok' as observation_status_key,
    n.source_version,
    n.raw_sha256
from newest as n
