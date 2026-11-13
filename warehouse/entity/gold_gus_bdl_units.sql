-- Gold build stage of one GUS BDL subgroup, step 3 (docs/gold-coverage.md): one row per territorial unit
-- of the subgroup (code + name, the Silver grain) with the area it maps to and its place among the units
-- of that area. Kept as the table `units`.
-- Relations: silver; geo_map (the result of gold_gus_bdl_map.sql: source_code, geography_key);
-- bdl_units (silver.bdl_units: unit_id, name, level).
-- A unit is mapped by its code: the 12-character BDL unit id or the 7-digit TERYT code of the older
-- website exports (Poland, voivodeship, powiat, gmina). BDL publishes one area under several units, and
-- two rows under one area would repeat series_key × date_key, so the units of an area have an order,
-- decided by rule and never by row order:
--   1. the area's own unit before a stand-in: a BDL region (level 3) or macroregion (level 1) that the
--      map places on a voivodeship comes after the voivodeship's unit
--   2. the unit with the area's own TERYT code before a unit with a code TERYT has changed since (BDL
--      keeps a gmina before and after it got town rights as two units)
--   3. the BDL unit id before the website's TERYT code of the same area
-- gold_gus_bdl_cells.sql then gives each cell of the area to the first unit that has it (`contested`:
-- the area has more than one unit in this block); the same cell of a later unit stays under `_X` with the
-- source code `<code>#<area>`, which says which area the code maps to. warehouse/observations.py reads
-- that form when the map changes. Two kinds of unit take no part and keep every row under that form
-- (plus `#<name>` when the code has several names in the block):
--   borrowed code  a unit whose name is not the name of its id is another unit under a borrowed code
--                  (the website export gives the Górnośląsko-Zagłębiowska Metropolia the id of Śląskie).
--                  The export writes a gmina with its type digit, "Bochnia (1)", the unit list without:
--                  the digit is not part of the name.
--   tied           two units the rule cannot order (two names under one TERYT code)
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
        coalesce(g.geography_key, '_X') as area,
        coalesce(
            upper(trim(u.name)) <> upper(trim(regexp_replace(h.unit_name, '\s*\([0-9]\)\s*$', ''))), false
        ) as borrowed,
        coalesce(u.level in (1, 3) and g.geography_key like 'TERC:%', false) as stand_in,
        coalesce(g.own_code, true) as own_code,
        length(h.unit_code) = 12 as bdl_id,
        count(*) over (partition by h.unit_code) as names_of_code
    from held as h
    left join bdl_units as u on h.unit_code = u.unit_id
    left join geo_map as g on h.unit_code = g.source_code
),

ranked as (
    select
        *,
        rank() over (partition by area order by borrowed asc, stand_in asc, own_code desc, bdl_id desc) as place
    from described
),

claiming as (
    select
        *,
        area <> '_X' and not borrowed and count(*) over (partition by area, place) = 1 as claims
    from ranked
)

select
    unit_code,
    unit_name,
    place,
    claims and count(*) filter (where claims) over (partition by area) > 1 as contested,
    case when claims then area else '_X' end as geography_key,
    case
        when claims or (area = '_X' and names_of_code = 1) then unit_code
        else unit_code || '#' || area || case when names_of_code > 1 then '#' || coalesce(unit_name, '') else '' end
    end as geography_source_code,
    unit_code || '#' || area as twin_code
from claiming
