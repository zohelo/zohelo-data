-- GIOŚ archive stations (silver.gios_air_archive_stations): the metadata workbook lists at least 1,000 stations of
-- all 16 voivodeships (1,148 in the workbook of Nov 2026, Dev probe 45747293591). A truncated or re-shaped sheet
-- fails here.
select
    count(*) as stations,
    count(distinct voivodeship) as voivodeships
from {{ ref('gios_air_archive_stations') }}
having count(*) < 1000 or count(distinct voivodeship) <> 16
