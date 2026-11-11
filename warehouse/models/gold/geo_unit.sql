{{ config(alias='geo_unit', pre_hook="install spatial; load spatial") }}
{# Gold geography block (#2739): one row per Polish administrative unit with a PRG boundary (the country,
   16 voivodeships, 380 powiats, 2,479 gminas): area as PRG publishes it (JPT_POWIER, hectares → km²),
   centroid in longitude/latitude (computed in PUWG 1992, EPSG:2180, then back to ETRS89), the number of
   neighbours of the same level (gold.geo_neighbour) and the PRG validity and edition. The geometry stays
   in silver.prg_units; simplified shapes are published as geo/<level>.geojson (docs/published.md). #}
with units as (
    select
        unit_level,
        teryt_code,
        name_pl,
        area_ha,
        valid_from,
        source_version,
        st_transform(st_geomfromwkb(geometry_wkb), 'EPSG:4258', 'EPSG:2180', always_xy := true) as geom_2180
    from {{ ref('prg_units') }}
    where unit_level in ('kraj', 'wojewodztwo', 'powiat', 'gmina')
),

neighbours as (
    select
        geography_key,
        count(*) as neighbours
    from {{ ref('geo_neighbour') }}
    group by geography_key
),

centred as (
    select
        *,
        st_transform(st_centroid(geom_2180), 'EPSG:2180', 'EPSG:4258', always_xy := true) as centroid
    from units
)

select
    case when c.unit_level = 'kraj' then 'PL' else 'TERC:' || c.teryt_code end as geography_key,
    case when c.unit_level = 'kraj' then 'country' else c.unit_level end as level_name,
    case when c.unit_level = 'kraj' then null else c.teryt_code end as terc_code,
    c.name_pl,
    c.area_ha / 100 as area_km2,
    round(st_x(c.centroid), 6) as centroid_lon,
    round(st_y(c.centroid), 6) as centroid_lat,
    coalesce(n.neighbours, 0) as neighbours,
    c.valid_from as prg_valid_from,
    c.source_version as prg_source_version
from centred as c
left join
    neighbours as n
    on n.geography_key = case when c.unit_level = 'kraj' then 'PL' else 'TERC:' || c.teryt_code end
