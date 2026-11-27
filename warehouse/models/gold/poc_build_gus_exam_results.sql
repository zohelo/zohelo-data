{{ config(alias='build_gus_exam_results', tags=['gold_block']) }}
{# Gold build stage: GUS republication of the CKE/OKE results (register PLS-025, #449), block 'exam8', from
   silver.gus_exam_results_eighth_grade. One measure per subject, annual only: measure_key
   gus_exam_results:exam8:pct_max_points:<subject>, e.g. gus_exam_results:exam8:pct_max_points:mathematics (mean result
   of the eighth-grade exam in % of the maximum number of points). Sex is the breakdown {"sex": "T" | "F" | "M"} and
   sex_code; ogółem (T) is the total (is_total). Values and units as published (D33). Areas through poc_map_geography
   (section gus_exam_results: the TERYT voivodeship names, 'Ogółem' = Poland). Rows with a GUS sign have no value and
   are not loaded ('(.)' no or unreliable data, '(–)' did not occur), so every loaded row is 'ok'. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gus_exam_results'
)

select
    'gus_exam_results' as source_system,
    'exam8' as block_id,
    'GUS: mean eighth-grade exam results (CKE/OKE)' as block_name,
    'gus_exam_results:exam8:' || s.unit_code || ':' || s.subject_code as measure_key,
    'Mean eighth-grade exam result, ' || s.subject || ' (% of points)' as measure_name,
    s.subject_code as measure_codes,
    s.unit_code,
    '% of the maximum number of points' as unit_name,
    s.area_name as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(s.period_year, 1, 1) as date_key,
    cast(s.period_year as varchar) as period_label,
    cast(json_object('sex', s.sex_code) as varchar) as breakdown_json,
    cast(json_object('sex', s.sex) as varchar) as breakdown_label_json,
    s.sex_code,
    s.sex_code = 'T' as is_total,
    s.value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('gus_exam_results_eighth_grade') }} as s
left join geo as g on lower(s.area_name) = g.source_code
where s.value is not null
