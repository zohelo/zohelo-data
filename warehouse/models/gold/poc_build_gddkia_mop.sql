{{ config(alias='build_gddkia_mop', tags=['gold_block']) }}
{# Gold build stage (#2995): rest areas (MOP) and parkings on national roads listed in each monthly edition of GDDKiA's
   "Tabela MOP istniejące" (silver.gddkia_mop_rest_areas), one measure gddkia_mop:rest_areas:facilities (a count of
   listed rest areas), breakdown current_category as published (and _T all), monthly (date_key = first day of the
   edition month, a state of that month). Areas: Poland (every row) and the voivodeship printed in the row (each rest
   area counts once, so the voivodeships add up to Poland); a name that matches no voivodeship meets in '_X' with the
   name kept. #}
with rest_areas as (
    select
        edition_month,
        voivodeship_name,
        current_category,
        source_version,
        raw_sha256
    from {{ ref('gddkia_mop_rest_areas') }}
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'gddkia_mop'
),

areas as (
    select
        r.*,
        'PL' as geo_code
    from rest_areas as r
    union all
    select
        r.*,
        r.voivodeship_name as geo_code
    from rest_areas as r
),

located as (
    select
        a.*,
        coalesce(g.geography_key, '_X') as geography_key
    from areas as a
    left join geo_map as g on a.geo_code = g.source_code
),

{% set cuts = ['current_category', "'_T'"] %}
counted as (
    {% for cut in cuts %}
        {% if not loop.first %}union all{% endif %}
        select
            edition_month,
            geography_key,
            string_agg(distinct geo_code, ';' order by geo_code) as geo_code,
            {{ cut }} as current_category,
            count(*) as rest_areas,
            max(source_version) as source_version,
            sha256(string_agg(distinct raw_sha256, ',' order by raw_sha256)) as files_sha256
        from located
        group by edition_month, geography_key{% if cut == 'current_category' %}, current_category{% endif %}
    {% endfor %}
)

select
    'gddkia_mop' as source_system,
    'rest_areas' as block_id,
    'GDDKiA rest areas (MOP) on national roads' as block_name,
    'gddkia_mop:rest_areas:facilities' as measure_key,
    'Rest areas (MOP) and parkings on national roads listed in the GDDKiA MOP table' as measure_name,
    'rest_areas' as measure_codes,
    'facilities' as unit_code,
    'facilities' as unit_name,
    geo_code as geography_source_code,
    geography_key,
    'M' as frequency,
    edition_month as date_key,
    strftime(edition_month, '%Y-%m') as period_label,
    cast(json_object('current_category', current_category) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    current_category = '_T' as is_total,
    cast(rest_areas as double) as value,
    'ok' as observation_status_key,
    source_version,
    files_sha256 as raw_sha256
from counted
