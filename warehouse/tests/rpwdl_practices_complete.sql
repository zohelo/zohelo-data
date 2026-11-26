-- rpwdl_practices (register PLS-020): both ledger files (active, inactive) are present and active rows cover 16
-- voivodeships (voivodeship_code comes from silver.teryt_terc, so no code list is typed here). A register file that
-- stops loading or a TERYT column that changes meaning fails here instead of thinning the counts silently. Refs only
-- this model (one test per Silver model).
-- Checked per snapshot year (#3139): every year kept must hold both files and all 16 voivodeships; an empty model
-- fails too.
with found as (
    select
        snapshot_year,
        count(distinct voivodeship_code) filter (where is_active) as voivodeships,
        count(*) filter (where ledger_state = 'active') as active_rows,
        count(*) filter (where ledger_state = 'inactive') as inactive_rows
    from {{ ref('rpwdl_practices') }}
    group by snapshot_year
)

select *
from found
where voivodeships <> 16 or active_rows = 0 or inactive_rows = 0
union all
select  -- no year at all (an empty model)
    null as snapshot_year,
    0 as voivodeships,
    0 as active_rows,
    0 as inactive_rows
where not exists (select 1 from found)
