-- Step of the GUS DBW Gold build: the rows of a NUTS twin that repeat a voivodeship row. Some DBW
-- cross-sections ("Polska, makroregiony, województwa, regiony") list a voivodeship and the NUTS region or
-- macroregion that is the same area ("DOLNOŚLĄSKIE" and "REGION DOLNOŚLĄSKIE", "MAZOWIECKIE" and
-- "MAKROREGION WOJEWÓDZTWO MAZOWIECKIE"). Gold has one area for both (the voivodeship, docs/model.md), so
-- the twin's row would repeat the key of the voivodeship's row. The rule of the curated poc_build_dbw
-- (#2759), with a guard: a twin row is left out only when the same file holds the voivodeship's row for
-- the same presentation, period, year and other positions WITH THE SAME VALUE. A twin row with another
-- value is kept, repeats the key and rejects the block (warehouse/observations.py): nothing is picked.
-- One row per left-out Silver row, by its identity; empty for a block without twins (the Silver files are
-- then not read a second time: the join below has nothing on its small side).
with twinned as (  -- the positions of the areas that have a twin in this block: the twins and their voivodeships
    select
        a.position_id,
        a.geography_key,
        a.is_twin
    from areas as a
    where a.geography_key in (
        select t.geography_key from areas as t
        where t.is_twin
    )
),

candidates as (
    select
        s.entity_id,
        s.presentation_id,
        s.period_id,
        s.year,
        s.value,
        a.geography_key,
        a.is_twin,
        [
            s.position_1_id,
            s.position_2_id,
            s.position_3_id,
            s.position_4_id,
            s.position_5_id,
            s.position_6_id,
            s.position_7_id,
            s.position_8_id,
            s.position_9_id
        ] as positions,
        [
            case when sh.territory_slot = 1 then null else s.position_1_id end,
            case when sh.territory_slot = 2 then null else s.position_2_id end,
            case when sh.territory_slot = 3 then null else s.position_3_id end,
            case when sh.territory_slot = 4 then null else s.position_4_id end,
            case when sh.territory_slot = 5 then null else s.position_5_id end,
            case when sh.territory_slot = 6 then null else s.position_6_id end,
            case when sh.territory_slot = 7 then null else s.position_7_id end,
            case when sh.territory_slot = 8 then null else s.position_8_id end,
            case when sh.territory_slot = 9 then null else s.position_9_id end
        ] as other_positions
    from silver as s
    cross join shape as sh
    inner join twinned as a
        on
            case sh.territory_slot
                when 1 then s.position_1_id
                when 2 then s.position_2_id
                when 3 then s.position_3_id
                when 4 then s.position_4_id
                when 5 then s.position_5_id
                when 6 then s.position_6_id
                when 7 then s.position_7_id
                when 8 then s.position_8_id
                when 9 then s.position_9_id
            end
            = a.position_id
    where s.value is not null and (coalesce(s.missing_reason_id, 253) = 253 or s.value <> 0)
)

select distinct
    t.entity_id,
    t.presentation_id,
    t.period_id,
    t.year,
    t.positions
from candidates as t
inner join candidates as v
    on
        t.geography_key = v.geography_key
        and t.entity_id = v.entity_id
        and t.presentation_id = v.presentation_id
        and t.period_id = v.period_id
        and t.year = v.year
        and t.other_positions = v.other_positions
        and t.value = v.value
where t.is_twin and not v.is_twin
