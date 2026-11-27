-- senat (PLG-025): the Senate has 100 seats; the senators list shows between 90 and 100 senators in office (a seat
-- can be vacant until a by-election) and a date for every mandate that ended. One test per Silver model.
with senators as (
    select * from {{ ref('senat_senators') }}
)

select
    'senators in office: ' || count(*) filter (where in_office) as problem
from senators
having count(*) filter (where in_office) not between 90 and 100
union all
select 'mandate ended without a date: ' || senator_id as problem
from senators
where not in_office and mandate_ended_on is null
