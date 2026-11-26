-- rpwdl_establishments (register PLS-020): both ledger files (active, inactive) are present and active rows cover 16
-- voivodeships (voivodeship_code comes from silver.teryt_terc, so no code list is typed here). A register file that
-- stops loading or a TERYT column that changes meaning fails here instead of thinning the counts silently. Refs only
-- this model (one test per Silver model).
with found as (
    select
        count(distinct voivodeship_code) filter (where is_active) as voivodeships,
        count(*) filter (where ledger_state = 'active') as active_rows,
        count(*) filter (where ledger_state = 'inactive') as inactive_rows
    from {{ ref('rpwdl_establishments') }}
)

select *
from found
where voivodeships <> 16 or active_rows = 0 or inactive_rows = 0
