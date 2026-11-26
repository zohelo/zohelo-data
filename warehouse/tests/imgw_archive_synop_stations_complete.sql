-- IMGW synoptic station dimension: the newest month of the archive has at least 50 stations (59 in October 2026;
-- 57 to 63 a year since 1966, Dev probe 43336937240). Fewer means a month's files were not loaded whole.
with newest as (
    select date_trunc('month', max(last_date)) as month_start
    from {{ ref('imgw_archive_synop_stations') }}
)

select
    n.month_start,
    count(s.station_code) as stations
from newest as n
left join {{ ref('imgw_archive_synop_stations') }} as s on s.last_date >= n.month_start
group by n.month_start
having count(s.station_code) < 50
