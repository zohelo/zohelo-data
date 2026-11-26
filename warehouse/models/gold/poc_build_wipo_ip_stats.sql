{{ config(alias='build_wipo_ip_stats', tags=['gold_block']) }}
{# Gold build stage: WIPO IP Statistics (register GL-032), four blocks, one per published count file:
   patent_publications and patent_grants (silver.wipo_ip_stats_patent_publications, _patent_grants; unit as published:
   publications, grants), design_applications and design_registrations (silver.wipo_ip_stats_designs; unit designs,
   the design count). Annual only. Filing office and applicant origin are two measures, never summed together:
   measure_key wipo_ip_stats:<block>:<unit>:by_office (geography = the office that received the filing; the published
   counts summed over applicant origins, '**' not stated included) and wipo_ip_stats:<block>:<unit>:by_origin
   (geography = the applicant's country of residence; summed over offices, regional offices and '**' included), as
   WIPO itself defines office and origin counts. The breakdown is the published class: {"technology_field": "<0-35>"}
   for patents, {"locarno_class": "<0-32>"} for designs. Patents also get a total row per area and year,
   {"technology_field": "_T"} (is_total): the sum of the 35 fields and 0 'Unknown', each document being counted in one
   field (2022: US 323,388, CN 798,431, PL 2,222; a WIPO technology count, not the office's own annual total: PL 2016
   is 3,776 here and 3,548 in the UPRP annual report, so the KPIs are their own concepts). Designs get no total: the design
   files hold only the offices that report by class, so a sum is not an office total. Values and units as published
   (D33). Areas through poc_map_geography (section wipo_ip_stats, static ISO2); regional offices (EP, EA, AP, OA, GC,
   EM, BX), historical codes (CS, YU, DD, AN), other codes without a Gold country and '**' have no Gold country and map to _X with their code kept
   (counted by build_check). #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'wipo_ip_stats'
),

counts as (
    select
        'patent_publications' as block_id,
        'publications' as unit_code,
        'technology_field' as class_dimension,
        office_code,
        origin_code,
        technology_field_code as class_code,
        technology_field as class_label,
        year,
        value,
        source_version,
        raw_sha256
    from {{ ref('wipo_ip_stats_patent_publications') }}
    union all
    select
        'patent_grants' as block_id,
        'grants' as unit_code,
        'technology_field' as class_dimension,
        office_code,
        origin_code,
        technology_field_code as class_code,
        technology_field as class_label,
        year,
        value,
        source_version,
        raw_sha256
    from {{ ref('wipo_ip_stats_patent_grants') }}
    union all
    select
        case measure when 'applications' then 'design_applications' else 'design_registrations' end as block_id,
        'designs' as unit_code,
        'locarno_class' as class_dimension,
        office_code,
        origin_code,
        locarno_class as class_code,
        'Locarno class ' || locarno_class as class_label,
        year,
        value,
        source_version,
        raw_sha256
    from {{ ref('wipo_ip_stats_designs') }}
),

by_class as (
    select
        block_id,
        unit_code,
        class_dimension,
        'by_office' as view_code,
        office_code as area_code,
        class_code,
        class_label,
        year,
        source_version,
        raw_sha256,
        sum(value) as value
    from counts
    group by all
    union all
    select
        block_id,
        unit_code,
        class_dimension,
        'by_origin' as view_code,
        origin_code as area_code,
        class_code,
        class_label,
        year,
        source_version,
        raw_sha256,
        sum(value) as value
    from counts
    group by all
),

obs as (
    select * from by_class
    union all
    select
        block_id,
        unit_code,
        class_dimension,
        view_code,
        area_code,
        '_T' as class_code,
        'Total (sum of the technology fields)' as class_label,
        year,
        source_version,
        raw_sha256,
        sum(value) as value
    from by_class
    where class_dimension = 'technology_field'
    group by all
)

select
    'wipo_ip_stats' as source_system,
    o.block_id,
    case o.block_id
        when 'patent_publications' then 'WIPO patent publications by office, origin and technology field'
        when 'patent_grants' then 'WIPO patent grants by office, origin and technology field'
        when 'design_applications' then 'WIPO industrial designs in applications by office, origin and Locarno class'
        else 'WIPO industrial designs in registrations by office, origin and Locarno class'
    end as block_name,
    'wipo_ip_stats:' || o.block_id || ':' || o.unit_code || ':' || o.view_code as measure_key,
    case o.block_id
        when 'patent_publications' then 'Patent publications'
        when 'patent_grants' then 'Patent grants'
        when 'design_applications' then 'Industrial designs in applications (design count)'
        else 'Industrial designs in registrations (design count)'
    end
    || case o.view_code
        when 'by_office' then ' at the filing office, all applicant origins (WIPO)'
        else ' by applicant origin, all offices (WIPO)'
    end as measure_name,
    o.view_code as measure_codes,
    o.unit_code,
    case o.unit_code
        when 'publications' then 'patent publications'
        when 'grants' then 'patent grants'
        else 'industrial designs'
    end as unit_name,
    o.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(o.year, 1, 1) as date_key,
    cast(o.year as varchar) as period_label,
    cast(json_object(o.class_dimension, o.class_code) as varchar) as breakdown_json,
    cast(json_object(o.class_dimension, o.class_label) as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    o.class_code = '_T' as is_total,
    cast(o.value as double) as value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from obs as o
left join geo as g on o.area_code = g.source_code
