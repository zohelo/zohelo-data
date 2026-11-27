-- PIG-PIB CBDG boreholes (silver.pig_boreholes_boreholes): the layer is present and not cut short. The WFS answered
-- 336,758 features on 26 Nov 2026 (resultType=hits, Dev trial 45899934080: 336,758 Silver rows); Landing checks every download against
-- its hits count, so fewer than the minimum below means Bronze lost pages (a rejected page, a partial load).
select count(*) as boreholes
from {{ ref('pig_boreholes_boreholes') }}
having count(*) < 320000
