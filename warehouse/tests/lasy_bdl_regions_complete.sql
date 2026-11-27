-- Lasy Państwowe BDL (silver.lasy_bdl_regions): all 17 regional directorates of the State Forests are present
-- (Dev probe 45896744643: resultType=hits 17 on 26 Nov 2026).
select count(*) as regions
from {{ ref('lasy_bdl_regions') }}
having count(*) < 17
