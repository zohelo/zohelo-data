-- GUS BDL geography map for the per-block Gold build (docs/gold-coverage.md): source code -> geography_key,
-- one row per code. Run once per run by warehouse/observations.py over the source's lookups; the build
-- stage and the builder's check of a changed map both read this result. Relations:
--   geo_map     gold.map_geography rows of GUS BDL (source_code, geography_key)
--   bdl_units   silver.bdl_units (unit_id, name, level)
-- A BDL unit id carries the TERYT code of its area (positions 3-4 voivodeship, 8-9 powiat, 10-11 gmina,
-- 12 gmina type), so Poland, a voivodeship, a powiat and a gmina are mapped by that code, through the map's
-- rows of the 7-digit TERYT codes (the website export's codes, generated from TERYT). The map's own rows of
-- these ids are matched by name and are not used: on the units of 47 subgroups the two agree for every
-- voivodeship and powiat, the name leaves 1,680 of 4,180 gmina ids unmatched and gives 8 gminas the area
-- of another gmina of the same name (Dev run 43329845553). A code TERYT no longer has (a unit that was
-- dissolved or renumbered) is unmatched: `_X`, never matched by name. Macroregions, regions and
-- subregions have no TERYT code and keep the map's row of their id.
with ids as (
    select
        unit_id,
        case
            when unit_id = '000000000000' then '0000000'
            when substr(unit_id, 10, 3) <> '000'
                then substr(unit_id, 3, 2) || substr(unit_id, 8, 2) || substr(unit_id, 10, 3)
            when substr(unit_id, 8, 2) <> '00' then substr(unit_id, 3, 2) || substr(unit_id, 8, 2) || '000'
            when substr(unit_id, 5, 8) = '00000000' and substr(unit_id, 3, 2) <> '00'
                then substr(unit_id, 3, 2) || '00000'
        end as teryt_code
    from bdl_units
    where length(unit_id) = 12
)

select
    m.source_code,
    m.geography_key
from geo_map as m
where
    m.source_code not in (
        select i.unit_id
        from ids as i
        where i.teryt_code is not null
    )
union all
select
    i.unit_id as source_code,
    coalesce(t.geography_key, '_X') as geography_key
from ids as i
left join geo_map as t on i.teryt_code = t.source_code
where i.teryt_code is not null
