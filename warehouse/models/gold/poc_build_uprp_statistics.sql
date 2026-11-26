{{ config(alias='build_uprp_statistics', tags=['gold_block']) }}
{# Gold build stage: UPRP industrial-property statistics (register EX-001), silver.uprp_statistics_counts.
   Block 'annual_reports': one measure_key per table and row (frequency A only; date_key = 1 January of the year),
   counts as published in single units: applications (unit 'applications'), rights granted or registered and
   European patents validated (unit 'rights'). Poland only (geography 'polska', mapped in poc_geography__uprp_statistics).
   Values of the newest report holding a checked cell (period_label names the report). A separate series from GUS BDL
   P3005 (patents granted by UPRP, a different scope; O31: never mixed). #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'uprp_statistics'
),

labelled as (
    select
        s.*,
        case when s.event_code = 'applications' then 'applications' else 'rights' end as unit_code,
        case s.table_code
            when 'inventions_applications' then 'Invention applications filed'
            when 'inventions_grants' then 'Patents for inventions granted'
            when 'utility_models_applications' then 'Utility model applications filed'
            when 'utility_models_grants' then 'Utility model protection rights granted'
            when 'european_patents_validations' then 'European patents validated in Poland'
            when 'trademarks_applications' then 'Trademark applications filed'
            when 'trademarks_grants' then 'Trademark protection rights granted'
            when 'industrial_designs_applications' then 'Industrial design applications filed'
            when 'industrial_designs_grants' then 'Industrial design registration rights granted'
        end
        || case s.row_code
            when 'total' then ''
            when 'national' then ', national procedure'
            when 'national_domestic' then ', national procedure, domestic applicants'
            when 'national_foreign' then ', national procedure, foreign applicants'
            when 'international' then ', international procedure'
            when 'international_domestic' then ', international procedure, domestic applicants'
            when 'international_foreign' then ', international procedure, foreign applicants'
            when 'domestic' then ', domestic applicants'
            when 'foreign' then ', foreign applicants'
        end as measure_text
    from {{ ref('uprp_statistics_counts') }} as s
)

select
    'uprp_statistics' as source_system,
    'annual_reports' as block_id,
    'UPRP industrial-property statistics (annual reports)' as block_name,
    'uprp_statistics:annual_reports:' || s.unit_code || ':' || s.table_code || ':' || s.row_code as measure_key,
    s.measure_text || ' (UPRP annual report)' as measure_name,
    s.table_code || ':' || s.row_code as measure_codes,
    s.unit_code,
    s.unit_code as unit_name,
    'polska' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(s.year, 1, 1) as date_key,
    s.year || ' (raport roczny ' || s.report_year || ')' as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(s.value as double) as value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from labelled as s
left join geo as g on g.source_code = 'polska'
where s.value is not null
