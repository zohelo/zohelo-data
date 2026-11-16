-- Step of the GUS DBW Gold build: the block's territorial dimension and its positions as areas. DBW has no
-- territorial codes: a position is an id with a name ("POLSKA", "DOLNOŚLĄSKIE", "REGION DOLNOŚLĄSKIE",
-- "MAKROREGION POŁUDNIOWY", "PODREGION KRAKOWSKI", "Powiat bielski"). The territorial dimension is the
-- dimension slot with the most positions named like Polish areas (then the largest share of them, then the
-- first slot): a slot of countries that only holds "Polska" loses to the slot of the reporting area. The
-- rule reads the block's own dictionary only, so a changed geography map changes keys, never the slot.
--   geography_source_code = <NAME IN UPPER CASE> #<position id>: the id keeps same-name areas apart (two
--     powiaty are "Powiat bielski"), the name is the key of gold.map_geography (`geo_map`);
--   geography_key by name through the map: Poland, voivodeships (TERC), NUTS macroregions, regions and
--     subregions; other names (powiaty, gminas, countries) are _X with the code kept;
--   is_twin: a NUTS region or macroregion that is one voivodeship ("REGION DOLNOŚLĄSKIE", "MAKROREGION
--     WOJEWÓDZTWO MAZOWIECKIE") and therefore maps to the voivodeship's key (gold_gus_dbw_twins.sql).
with positions as (
    select
        cast(right(column_name, 1) as integer) as slot,
        try_cast(element_id as bigint) as position_id,
        coalesce(upper(trim(label)), '') as name
    from labels
    where regexp_matches(column_name, '^id_pozycja_[1-9]$')
),

slots as (
    select
        slot,
        count(*) as positions,
        count(*) filter (
            where
            name in (
                'POLSKA', 'DOLNOŚLĄSKIE', 'KUJAWSKO-POMORSKIE', 'LUBELSKIE', 'LUBUSKIE', 'ŁÓDZKIE', 'MAŁOPOLSKIE',
                'MAZOWIECKIE', 'OPOLSKIE', 'PODKARPACKIE', 'PODLASKIE', 'POMORSKIE', 'ŚLĄSKIE', 'ŚWIĘTOKRZYSKIE',
                'WARMIŃSKO-MAZURSKIE', 'WIELKOPOLSKIE', 'ZACHODNIOPOMORSKIE'
            )
            or regexp_matches(name, '^(MAKROREGION|REGION|PODREGION|POWIAT) ')
        ) as area_positions
    from positions
    where position_id is not null
    group by slot
),

territory as (
    select slot
    from slots
    where area_positions > 0
    order by area_positions desc, area_positions / positions desc, slot asc
    limit 1
),

mapped as (  -- a name the map lists twice with one key is one mapping; with two keys it stays two rows,
    -- repeats the block's rows and rejects the block (gold_geography.sql: geography_keys > 1)
    select distinct
        source_code,
        geography_key
    from geo_map
)

select
    p.slot,
    p.position_id,
    p.name || ' #' || cast(p.position_id as varchar) as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    regexp_matches(p.name, '^(REGION|MAKROREGION) ')
    and regexp_matches(coalesce(g.geography_key, ''), '^TERC:[0-9]{2}$') as is_twin
from positions as p
inner join territory as t on p.slot = t.slot
left join mapped as g on p.name = g.source_code
where p.position_id is not null
