-- sejm (PLG-024): the Sejm has 460 seats, so between 440 and 460 MPs of term 10 are active (a seat stays empty only
-- until the next candidate takes the oath); fewer or more means a cut-off or changed list. One test per Silver
-- model (it reads no other model).
select
    count(*) filter (where is_active) as active_mps,
    'active MPs outside 440..460' as problem
from {{ ref('sejm_mps') }}
having count(*) filter (where is_active) not between 440 and 460
