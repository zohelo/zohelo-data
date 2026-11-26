-- GIOŚ air quality (gold.build_gios_air_quality): every daily mean of PM10, PM2.5, NO2, O3 and SO2 lies within
-- 0 to 1,000 µg/m³ for every area. The highest daily means GIOŚ reports for Polish stations are a few hundred µg/m³
-- (PM10 in winter smog episodes); a value above 1,000 or below 0 is a unit or decoding error (a value in ng/m³, a
-- missing value read as a number). A live test on the build stage, as no metrics model of these KPIs is in
-- tests/metrics_value_range.sql's union. Measured range: docs/sources/gios_air_quality.md. Returns the rows outside.
select
    measure_key,
    geography_key,
    date_key,
    value
from {{ ref('poc_build_gios_air_quality') }}
where value < 0 or value > 1000
