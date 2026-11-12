-- Step of the GUS DBW Gold build: one row with what every row of the block needs besides its own ids.
--   territory_slot  the dimension slot that is the territory (gold_gus_dbw_areas.sql); null: none
--   totals          positions that are the total of their dimension, as '<slot>:<position id>': named
--                   "ogółem" or "razem" (also behind a code, "0 - OGÓŁEM"), or the only position of the slot
--   men, women, sex_totals   positions named "mężczyźni", "kobiety", and the total of a dimension "Płeć"
with positions as (
    select
        cast(right(column_name, 1) as integer) as slot,
        cast(right(column_name, 1) as varchar) || ':' || element_id as slot_position,
        lower(trim(coalesce(label, ''))) as name
    from labels
    where regexp_matches(column_name, '^id_pozycja_[1-9]$') and try_cast(element_id as bigint) is not null
),

sex_slots as (
    select distinct cast(right(column_name, 1) as integer) as slot
    from labels
    where regexp_matches(column_name, '^id_wymiar_[1-9]$') and lower(trim(label)) = 'płeć'
),

marked as (
    select
        p.slot_position,
        p.name in ('ogółem', 'razem') as named_total,
        regexp_matches(p.name, '^[0-9a-z]+ - (.* )?ogółem$') as coded_total,
        count(*) over (partition by p.slot) = 1 as only_position,
        p.name = 'mężczyźni' as is_men,
        p.name = 'kobiety' as is_women,
        p.name in ('ogółem', 'razem') and s.slot is not null as is_sex_total
    from positions as p
    left join sex_slots as s on p.slot = s.slot
)

select
    (select min(a.slot) from areas as a) as territory_slot,
    coalesce(
        list(slot_position) filter (where named_total or coded_total or only_position), cast([] as varchar[])
    ) as totals,
    coalesce(list(slot_position) filter (where is_men), cast([] as varchar[])) as men,
    coalesce(list(slot_position) filter (where is_women), cast([] as varchar[])) as women,
    coalesce(list(slot_position) filter (where is_sex_total), cast([] as varchar[])) as sex_totals
from marked
