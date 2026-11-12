-- Gold build stage of one GUS BDL subgroup, step 2 (docs/gold-coverage.md): one row per territorial unit
-- of the subgroup (code + name, the Silver grain) with the area it gets. Kept as the table `units`.
-- Relations: silver; geo_map (gold.map_geography rows of GUS BDL: source_code, geography_key);
-- bdl_units (silver.bdl_units: unit_id, name, level).
-- A unit is mapped by its code: the 12-character BDL unit id or the 7-digit TERYT code of the website
-- export (Poland, voivodeship, powiat, gmina). One area has one unit in a block, decided by rule and never
-- by row order, because two units under one area would repeat series_key × date_key:
--   borrowed code  a unit whose name is not the name of its id is another unit under a borrowed code (the
--                  website export gives the Górnośląsko-Zagłębiowska Metropolia the id of Śląskie): it
--                  never holds the area
--   stand-in       a BDL region (level 3) or macroregion (level 1) that the map places on a voivodeship
--                  gives way to the voivodeship's own unit
--   two codes      the BDL unit id comes before the website's TERYT code of the same area
--   still tied     no unit holds the area
-- A unit that does not hold its area keeps every row: geography_key `_X`, and its source code says which
-- area it maps to, `<code>#<area>` (plus `#<name>` when the code has several names in the block), so no
-- row is dropped and no value is chosen. warehouse/observations.py reads that form when the map changes.
with held as (
    select distinct
        unit_code,
        unit_name
    from silver
    where value is not null
),

described as (
    select
        h.unit_code,
        h.unit_name,
        coalesce(g.geography_key, '_X') as mapped,
        coalesce(u.name is not null and upper(trim(u.name)) <> upper(trim(h.unit_name)), false) as borrowed,
        coalesce(u.level in (1, 3) and g.geography_key like 'TERC:%', false) as stand_in,
        length(h.unit_code) = 12 as bdl_id,
        count(*) over (partition by h.unit_code) as names_of_code
    from held as h
    left join bdl_units as u on h.unit_code = u.unit_id
    left join geo_map as g on h.unit_code = g.source_code
),

ranked as (
    select
        *,
        rank() over (partition by mapped order by borrowed asc, stand_in asc, bdl_id desc) as place
    from described
),

placed as (
    select
        *,
        mapped <> '_X'
        and not borrowed
        and place = 1
        and count(*) filter (where place = 1) over (partition by mapped) = 1 as holds
    from ranked
)

select
    unit_code,
    unit_name,
    case when holds then mapped else '_X' end as geography_key,
    case
        when holds or (mapped = '_X' and names_of_code = 1) then unit_code
        else unit_code || '#' || mapped || case when names_of_code > 1 then '#' || coalesce(unit_name, '') else '' end
    end as geography_source_code
from placed
