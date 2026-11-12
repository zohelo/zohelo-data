{{ config(alias='geo_neighbour', pre_hook="install spatial; load spatial") }}
{# Gold geography block (#2739): contiguity of Polish administrative units from the PRG boundaries
   (silver.prg_units, full resolution, ETRS89 geographic). One row per unit × neighbour of the same
   level (wojewodztwo, powiat, gmina), both directions, so a spatial weight matrix is a pivot of this
   table. shared_border_km: length of the common boundary in PUWG 1992 (EPSG:2180); contiguity 'rook'
   when they share a line (any length > 0), 'queen' when they touch in points only (13 gmina pairs in Nov 2026):
   here 'queen' marks the point-only pairs; a textbook queen matrix (rook + point contacts) takes every row. Gmina
   level: the 2,479 gminas of PRG (TERC types 1, 2, 3); the town and rural parts (4, 5) and the
   districts (8, 9) have no PRG boundary. #}
with units as (
    select
        unit_level as level_name,
        teryt_code,
        -- fails loudly if an edition ships in another CRS (e.g. projected EPSG:2180): the transforms assume EPSG:4258
        case
            when
                st_xmin(st_geomfromwkb(geometry_wkb)) between 13 and 25
                and st_ymax(st_geomfromwkb(geometry_wkb)) between 48 and 56
                then st_geomfromwkb(geometry_wkb)
            else error('PRG geometry ' || teryt_code || ' is not in EPSG:4258 degrees: check the edition CRS')
        end as geom
    from {{ ref('prg_units') }}
    where unit_level in ('wojewodztwo', 'powiat', 'gmina')
),

pairs as (
    select
        a.level_name,
        a.teryt_code as code_a,
        b.teryt_code as code_b,
        st_length(
            st_transform(st_intersection(a.geom, b.geom), 'EPSG:4258', 'EPSG:2180', always_xy := true)
        ) / 1000 as shared_border_km
    from units as a
    inner join units as b
        on a.level_name = b.level_name and a.teryt_code < b.teryt_code and st_intersects(a.geom, b.geom)
),

both_ways as (
    select
        level_name,
        code_a as unit_code,
        code_b as neighbour_code,
        shared_border_km
    from pairs
    union all
    select
        level_name,
        code_b as unit_code,
        code_a as neighbour_code,
        shared_border_km
    from pairs
)

select
    level_name,
    'TERC:' || unit_code as geography_key,
    'TERC:' || neighbour_code as neighbour_key,
    round(shared_border_km, 3) as shared_border_km,
    case when shared_border_km > 0 then 'rook' else 'queen' end as contiguity
from both_ways
