-- Lasy Państwowe BDL (silver.lasy_bdl_districts): the layer is present and not cut short. The WFS answered 429 forest
-- districts on 26 Nov 2026 (Dev probe 45896744643, resultType=hits the same); fewer than 400 (a truncated response
-- that still passed the Bronze layout checks) fails here.
select count(*) as districts
from {{ ref('lasy_bdl_districts') }}
having count(*) < 400
