{# Lasy Państwowe Bank Danych o Lasach (register PL-ENV-018), regional directorates of the State Forests (RDLP) from
   bronze.lasy_bdl_features: one row per feature of the newest copy of the WFS layer BDL:RDLP (entity regions).
   Grain: region_code (RDLP code, as published). Name as published (upper case, the seat of the directorate), data
   year (a_year), the planar area of the directorate's territorial range in EPSG:2180 in m² and its centroid and a
   point on its surface in degrees (EPSG:4326), computed in Bronze from the geometry (the layer has no area
   attribute); the geometry stays in Landing and the Archive. Shared SQL: macros/sources/lasy_bdl.sql. #}
with newest as (
    select *
    from {{ source('lasy_bdl', 'lasy_bdl_features') }}
    where entity = 'regions'
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    {{ poc_lasy_bdl_code('region_cd', 'lasy_bdl_regions') }} as region_code,
    {{ poc_lasy_bdl_text('region_name') }} as name,
    {{ poc_lasy_bdl_year('a_year', 'lasy_bdl_regions') }} as data_year,
    {{ poc_lasy_bdl_measures() }},
    feature_id,
    cast(strptime(source_version, '%Y%m%dT%H%M%SZ') as date) as snapshot_date,
    source_version,
    raw_sha256
from newest
