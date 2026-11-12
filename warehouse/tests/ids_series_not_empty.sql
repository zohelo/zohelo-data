-- World Bank IDS (#2756): silver.ids_series holds at least one series. The series member is found by name
-- (IDS_SeriesMetaData.csv); a renamed member would leave the table empty and the relationships test of
-- ids_observations.series_code would then fail on every row. Returns one row when the table is empty.
select count(*) as series_rows
from {{ ref('ids_series') }}
having count(*) = 0
