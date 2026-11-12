-- ZUS statistical portal: every workbook in Bronze (one entity = one table family × period) yields at least one Silver
-- row. A header or data-row text that the Silver model no longer finds (e.g. a renamed "Przeciętna wysokość na osobę"
-- row) would otherwise drop that period silently; returns the entities without Silver rows.
select distinct b.entity
from {{ source('zus_psz', 'zus_psz_cells') }} as b
left join (select distinct entity from {{ ref('zus_psz_observations') }}) as s on b.entity = s.entity
where s.entity is null
