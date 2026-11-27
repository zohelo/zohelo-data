{# Lasy Państwowe Bank Danych o Lasach (register PL-ENV-018), forest districts (nadleśnictwa) from
   bronze.lasy_bdl_features: one row per feature of the newest copy of the WFS layer BDL:Nadleśnictwa (entity
   districts). Grain: district_code = region_code || '-' || inspectorate_code (RDLP code and the district's code within
   it, as published; the forest address adress_forest starts with both, checked). Name (an organisational unit), data
   year (a_year), the planar area of the district's territorial range in EPSG:2180 in m² and its centroid and a point on
   its surface in degrees (EPSG:4326), computed in Bronze from the geometry (the layer has no area attribute); the
   geometry stays in Landing and the Archive. Shared SQL: macros/sources/lasy_bdl.sql. #}
with newest as (
    select *
    from {{ source('lasy_bdl', 'lasy_bdl_features') }}
    where entity = 'districts'
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    {{ poc_lasy_bdl_code('region_cd', 'lasy_bdl_districts') }} || '-'
    || {{ poc_lasy_bdl_code('inspectorate_cd', 'lasy_bdl_districts') }} as district_code,
    {{ poc_lasy_bdl_code('region_cd', 'lasy_bdl_districts') }} as region_code,
    {{ poc_lasy_bdl_code('inspectorate_cd', 'lasy_bdl_districts') }} as inspectorate_code,
    case
        when starts_with(adress_forest, region_cd || '-' || inspectorate_cd || '-') then adress_forest
        else error(
            'lasy_bdl_districts: forest address ' || coalesce(adress_forest, 'NULL') || ' does not start with '
            || coalesce(region_cd, 'NULL') || '-' || coalesce(inspectorate_cd, 'NULL')
        )
    end as forest_address,
    {{ poc_lasy_bdl_text('inspectorate_name') }} as name,
    {{ poc_lasy_bdl_year('a_year', 'lasy_bdl_districts') }} as data_year,
    {{ poc_lasy_bdl_measures() }},
    feature_id,
    cast(strptime(source_version, '%Y%m%dT%H%M%SZ') as date) as snapshot_date,
    source_version,
    raw_sha256
from newest
