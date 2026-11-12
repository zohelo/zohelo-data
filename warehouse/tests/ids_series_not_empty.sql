-- World Bank IDS (#2756): silver.ids_series holds series described by the archive's series member. The member
-- is found by name (IDS_SeriesMetaData.csv); a renamed member would leave only the series taken from the
-- observations (in_series_metadata false). Returns one row when no described series exists.
select count(*) as described_series
from {{ ref('ids_series') }}
where in_series_metadata
having count(*) = 0
