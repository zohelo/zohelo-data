-- GUGiK PRG address counts (gold.prg_address_counts, #3106): one row per unit_level × area_code, Poland once, all 16
-- voivodeships (TERC 02 … 32), every powiat and voivodeship code made of its gmina codes, and Poland equal to the
-- points counted (a point without a gmina code counts for Poland only, so Poland >= the sum of the voivodeships).
-- Reads only the source's own model. Rows returned = what breaks it.
with counts as (
    select * from {{ ref('poc_prg_address_counts') }}
),

repeated as (
    select
        'repeated key' as problem,
        unit_level || ':' || area_code as detail
    from counts
    group by unit_level, area_code
    having count(*) > 1
),

levels as (
    select
        count(*) filter (where unit_level = 'kraj') as poland,
        count(*) filter (where unit_level = 'wojewodztwo') as voivodeships,
        sum(address_points) filter (where unit_level = 'kraj') as poland_points,
        sum(address_points) filter (where unit_level = 'wojewodztwo') as voivodeship_points,
        sum(address_points) filter (where unit_level = 'powiat') as powiat_points,
        sum(address_points) filter (where unit_level = 'gmina') as gmina_points
    from counts
)

select * from repeated
union all
select
    'levels' as problem,
    format('poland {} voivodeships {} points PL {} W {} P {} G {}', poland, voivodeships, poland_points,
        voivodeship_points, powiat_points, gmina_points) as detail
from levels
where
    poland <> 1 or voivodeships <> 16 or voivodeship_points <> powiat_points or powiat_points <> gmina_points
    or poland_points < voivodeship_points
union all
select
    'not a voivodeship code' as problem,
    area_code as detail
from counts
where unit_level = 'wojewodztwo' and area_code not in (
    '02', '04', '06', '08', '10', '12', '14', '16', '18', '20', '22', '24', '26', '28', '30', '32'
)
