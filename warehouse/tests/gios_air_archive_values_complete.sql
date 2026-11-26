-- GIOŚ verified archive values (silver.gios_air_archive_values, written per year file by the loader, #3119): the
-- years held form one unbroken run (the loader works newest first, so a load stopped by its time budget leaves the
-- oldest years out, never a gap), and every year holds hourly NO2 values (published every year since 2000). Rows
-- returned = the years that break it.
with years as (
    select
        archive_year,
        count(*) filter (where indicator = 'NO2' and averaging_time = '1g') as no2_hourly
    from {{ source('gios_air_quality_silver', 'gios_air_archive_values') }}
    group by archive_year
),

bounds as (
    select
        min(archive_year) as first_year,
        max(archive_year) as last_year,
        count(*) as years_held
    from years
)

select
    y.archive_year,
    y.no2_hourly,
    b.years_held,
    b.last_year - b.first_year + 1 as years_expected
from years as y
cross join bounds as b
where y.no2_hourly = 0 or b.years_held <> b.last_year - b.first_year + 1
