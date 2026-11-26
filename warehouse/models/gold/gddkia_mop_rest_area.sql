{{ config(alias='gddkia_mop_rest_area') }}
{# Gold entity table (register source, playbook section 7): one row per rest area (MOP) or parking of the newest
   landed edition of GDDKiA's "Tabela MOP istniejące", keyed by the producer's row number Lp. of that edition (the
   table has no other id); from silver.gddkia_mop_rest_areas. The edition month is the date key (dim_calendar); the
   printed voivodeship is the geography key ('TERC:<code>', dim_geography) through map_geography (the same rule as
   gold.build_gddkia_mop; '_X' for a name that maps to no voivodeship). Values as published; the manager's name only
   for GDDKiA, "Prywatny" and companies (Silver). #}
with rest_areas as (
    select * from {{ ref('gddkia_mop_rest_areas') }}
    qualify edition_month = max(edition_month) over ()
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'gddkia_mop'
)

select
    r.lp,
    r.edition_month as date_key,
    r.branch,
    r.voivodeship,
    coalesce(g.geography_key, '_X') as geography_key,
    r.locality,
    r.rest_area_name,
    r.current_category,
    r.target_category,
    r.road_class,
    r.road_number,
    r.chainage,
    r.direction,
    r.x,
    r.y,
    r.manager_kind,
    r.manager_name,
    r.source_version,
    r.raw_sha256
from rest_areas as r
left join geo_map as g on r.voivodeship_name = g.source_code
