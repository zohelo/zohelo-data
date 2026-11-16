-- Gold build stage of one GUS BDL subgroup, step 4 (docs/gold-coverage.md): the cells (series × year) an
-- area gets from more than one of its units, one row per cell of a unit that is not the first to have it.
-- Kept as the table `cells`; a subgroup whose areas have one unit each gives no rows. Relations: silver,
-- units (step 3), signs (step 1). The units of an area are taken in the order of gold_gus_bdl_units.sql
-- (`place`): the first unit that has a cell gives the area its value. The same cell of a later unit is
-- listed here and stays in Gold under `_X` and the unit's own code, whatever its value: no row is dropped
-- and no value is chosen between two units. A cell only a later unit has goes to the area like any other
-- row and is not listed.
with claimed as (
    select
        s.unit_code,
        s.unit_name,
        s.dimension_values,
        s.year,
        row_number() over (partition by u.geography_key, s.dimension_values, s.year order by u.place) as pick
    from silver as s
    inner join units as u on s.unit_code = u.unit_code and s.unit_name is not distinct from u.unit_name
    left join signs as z on s.channel = z.channel and lower(s.attribute) = z.sign
    where
        u.contested
        and s.value is not null
        and not (s.value = 0 and z.sign is not null)
)

select
    unit_code,
    unit_name,
    dimension_values,
    year
from claimed
where pick > 1
