-- IMGW daily synop archive (silver.imgw_archive_synop_daily, written per ZIP by the loader): every station-day of the
-- s_d files carries all seven parameters (TMAX, TMIN, STD, TMNG, SMDB, PKSN, USL), measured or with its status; a
-- day with fewer means a field pair was lost in Silver. Rows returned = station-days that break it.
select
    station_code,
    observation_date,
    count(*) as parameters
from {{ source('imgw_archive_entities', 'imgw_archive_synop_daily') }}
group by station_code, observation_date
having count(*) <> 7
