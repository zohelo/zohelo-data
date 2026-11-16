{{ config(severity='warn') }}
-- IMGW API synop snapshots (silver.imgw_api_synop): every observation hour held has at least 40 of the 62 synoptic
-- stations the API lists (Dev probe 43336937240). Fewer means a snapshot was cut or the API served a partial list;
-- a warning, because the API's own outages must not stop the pipeline.
select
    observed_at,
    count(*) as stations
from {{ ref('imgw_api_synop') }}
group by observed_at
having count(*) < 40
