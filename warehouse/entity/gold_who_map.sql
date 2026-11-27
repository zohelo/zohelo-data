-- WHO data.who.int geography map for the per-block Gold build (docs/gold-coverage.md): source code
-- `<area type>:<UN M49 code>` (as Silver holds the code, without leading zeros) -> geography_key, one row per code.
-- Run once per run by warehouse/observations.py over the source's lookups; the build stage and the builder's check
-- of a changed map both read this result. Relations:
--   geo_map          gold.map_geography rows of WHO (poc_geography__who: every code in Silver; the EU27 members
--                    and GLOBAL:1 = World have a key, every other code _X)
--   areas            gold.dim_geography (geography_key, iso3, level_name)
--   iso3166_numeric  ISO 3166-1 numeric -> alpha-3 (gold_iso3166_numeric.sql)
-- An area of type COUNTRY, or of the two territory types WHO publishes (NATIONAL LIBERATION MOVEMENT,
-- ORGANIZED, UNINCORPORATED TERRITORY), is keyed by its M49 code, which for these areas is the ISO 3166-1 numeric
-- code: it is mapped through its ISO alpha-3 code to the country Gold has under that code. WHO regions, World Bank
-- income groups and other types have no Gold area and stay unmatched (_X, code kept). The keyed rows of the curated
-- map win where both have a code (its _X rows decide nothing); a code that would map to two areas is left out
-- (unmatched), never picked.
with curated as (
    select
        source_code,
        geography_key
    from geo_map
    where geography_key <> '_X'
),

by_code as (
    select
        t.area_type || ':' || cast(i.m49 as varchar) as source_code,
        a.geography_key
    from iso3166_numeric as i
    cross join (
        values ('COUNTRY'), ('NATIONAL LIBERATION MOVEMENT'), ('ORGANIZED, UNINCORPORATED TERRITORY')
    ) as t (area_type)
    inner join areas as a on i.iso3 = a.iso3 and a.level_name = 'country'
    where t.area_type || ':' || cast(i.m49 as varchar) not in (select c.source_code from curated as c)
),

candidates as (
    select source_code, geography_key from curated
    union all
    select source_code, geography_key from by_code
)

select
    source_code,
    min(geography_key) as geography_key
from candidates
group by source_code
having count(distinct geography_key) = 1
