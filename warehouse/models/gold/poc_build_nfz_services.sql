{{ config(alias='build_nfz_services', tags=['gold_block']) }}
{# Gold build stage: NFZ statistics API, services (silver.nfz_services_basic_data), whole-year tables only
   (frequency A; part-year tables stay in Silver). Block = catalog. Each benefit is its own measure (the API
   publishes no total over benefits, and a KPI selects one benefit by its NFZ product code):
   measure_key nfz_services:<catalog>:<unit code>:<benefit code>:<measure slug>, breakdown {}. Scope (macro
   poc_nfz_services_measures): hospitalisations and patients of every loaded benefit for Poland; every measure
   for Poland and the 16 NFZ branches of the benefits in poc_nfz_services_detail(). Values and units as published
   (D33); a null is not loaded. Branches map to voivodeships in poc_geography__nfz_services(). #}
with measures as ({{ poc_nfz_services_measures() }}),

detail as ({{ poc_nfz_services_detail() }}),

blocks as ({{ poc_nfz_services_blocks() }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'nfz_services'
),

silver as (
    select
        s.*,
        d.benefit_code is not null as is_detail
    from {{ ref('nfz_services_basic_data') }} as s
    left join detail as d on s.catalog = d.catalog and s.benefit_code = d.benefit_code
    where s.period_kind = 'year'
),

long as (
    select
        s.catalog,
        s.benefit_code,
        s.benefit_name,
        s.area_code,
        s.is_detail,
        s.year,
        s.period_start,
        s.source_version,
        s.raw_sha256,
        v.api_field,
        v.value
    from silver as s
    cross join lateral (
        values
        ('number-of-hospitalizations', cast(s.number_of_hospitalizations as double)),
        ('number-of-patients', cast(s.number_of_patients as double)),
        ('ratio-of-rehospitalizations', s.ratio_of_rehospitalizations),
        ('percentage', s.percentage),
        ('percentage-of-sections', s.percentage_of_sections),
        ('duration-of-hospitalization-mediana', cast(s.duration_median_days as double)),
        ('duration-of-hospitalization-mode', cast(s.duration_mode_days as double)),
        ('average-value-of-hospitalization', s.average_value_of_hospitalization_pln),
        ('average-value-of-hospitalization-points', s.average_value_of_hospitalization_points),
        ('average-value-of-drg', s.average_value_of_drg_pln),
        ('average-value-of-drg-points', s.average_value_of_drg_points)
    ) as v (api_field, value)
)

select
    'nfz_services' as source_system,
    o.catalog as block_id,
    b.block_name,
    'nfz_services:' || o.catalog || ':' || m.unit_code || ':' || o.benefit_code || ':' || m.measure_slug as measure_key,
    o.benefit_name || ' — ' || m.measure_label as measure_name,
    o.benefit_code || '|' || m.api_field as measure_codes,
    m.unit_code,
    m.unit_name,
    o.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    o.period_start as date_key,
    cast(o.year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    o.value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from long as o
inner join measures as m on o.api_field = m.api_field
inner join blocks as b on o.catalog = b.block_id
left join geo_map as g on o.area_code = g.source_code
where
    o.value is not null
    and (o.is_detail or (m.scope = 'all' and o.area_code = 'PL'))
