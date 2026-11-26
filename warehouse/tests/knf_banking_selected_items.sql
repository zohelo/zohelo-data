-- knf_banking: each of the seven items the Gold build stage picks by sheet and item path has rows for the whole banking
-- sector ('Sektor bankowy', is_total). A label that KNF renames would otherwise drop a measure silently; returns the
-- number of items found when it is not seven.
select count(distinct measure_codes) as items_found
from {{ ref('poc_build_knf_banking') }}
where is_total
having count(distinct measure_codes) <> 7
