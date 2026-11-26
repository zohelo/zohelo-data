-- PIG-PIB MIDAS (silver.pig_midas_mining_terrains): both layers are present and neither is cut short. The WFS answered
-- 6,001 current and 4,800 archival contours on 26 Nov 2026
-- (Dev probe 45818533713, resultType=hits the same); a layer with fewer than the minimum below (a truncated
-- response that still passed the Bronze layout checks) fails here.
with expected (layer, minimum) as (
    values ('mining_terrains', 5000), ('mining_terrains_archival', 4000)
),

counts as (
    select
        layer,
        count(*) as n
    from {{ ref('pig_midas_mining_terrains') }}
    group by layer
)

select
    e.layer,
    coalesce(c.n, 0) as contours
from expected as e
left join counts as c on e.layer = c.layer
where coalesce(c.n, 0) < e.minimum
