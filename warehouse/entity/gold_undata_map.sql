-- UNdata geography map for the per-block Gold build (docs/gold-coverage.md): UN M49 code (as Silver holds it,
-- without leading zeros) -> geography_key, one row per code. Run once per run by warehouse/observations.py
-- over the source's lookups; the build stage and the builder's check of a changed map both read this result.
-- Relations:
--   geo_map   gold.map_geography rows of UNdata (the EU27 members and 1 = World; the curated map)
--   areas     gold.dim_geography (geography_key, iso3, level_name)
-- For a country, M49 is the ISO 3166-1 numeric code (the UN Statistics Division assigns both), so a country
-- code is mapped through its ISO 3166-1 alpha-3 code to the country Gold has under that code; the
-- correspondence is ISO 3166-1 (numeric -> alpha-3, all 249 entries, the view iso3166_numeric of
-- gold_iso3166_numeric.sql). The rows of the curated map win where both have a code. A code of
-- an M49 region or other aggregate is in neither and stays unmatched (_X, code kept); a code that would map to
-- two areas is left out (unmatched), never picked.
with iso as (
    select * from iso3166_numeric
),

by_code as (
    select
        cast(i.m49 as varchar) as source_code,
        a.geography_key
    from iso as i
    inner join areas as a on i.iso3 = a.iso3 and a.level_name = 'country'
    where cast(i.m49 as varchar) not in (select m.source_code from geo_map as m)
),

candidates as (
    select source_code, geography_key from geo_map
    union all
    select source_code, geography_key from by_code
)

select
    source_code,
    min(geography_key) as geography_key
from candidates
group by source_code
having count(distinct geography_key) = 1
