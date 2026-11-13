-- rpwdl_providers (register PLS-020): both ledger files (active, inactive) are present and every one of the 16
-- voivodeships has active rows. A register file that stops loading or a TERYT column that changes meaning fails
-- here instead of thinning the counts silently. Refs only this model (one test per Silver model).
with expected as (
    select unnest(['02', '04', '06', '08', '10', '12', '14', '16', '18', '20', '22', '24', '26', '28', '30', '32']) as code
    union all
    select unnest(['ledger:active', 'ledger:inactive'])
),

found as (
    select distinct voivodeship_code as code
    from {{ ref('rpwdl_providers') }}
    where is_active and voivodeship_code is not null
    union all
    select distinct 'ledger:' || ledger_state
    from {{ ref('rpwdl_providers') }}
)

select e.code as missing
from expected as e
left join found as f on e.code = f.code
where f.code is null
