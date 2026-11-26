-- Silver: GIOŚ air-quality monitoring stations of the archive's metadata workbook ("Metadane oraz kody stacji i
-- stanowisk pomiarowych", sheet STACJE; register PL-ENV-010, #3119): every station since 2000, closed ones too.
-- Grain: station_code (the current code); the Bronze table holds the newest version of the workbook only.
-- Source: bronze.gios_air_archive_stations. old_station_codes as published: the codes the station had before
-- (o ile inny od aktualnego), under which the older year files list it. Dates are Excel serial day numbers in the
-- workbook (1899-12-30 = 0); international code (EoI), type, area type, voivodeship (upper case), locality and
-- WGS84 coordinates as published (-999, the workbook's mark for an unknown coordinate, is null).
select
    station_code,
    international_code,
    station_name,
    old_station_codes,
    cast(date '1899-12-30' + cast(try_cast(opened_on as double) as integer) as date) as opened_on,
    cast(date '1899-12-30' + cast(try_cast(closed_on as double) as integer) as date) as closed_on,
    station_type,
    area_type,
    station_kind,
    voivodeship,
    locality,
    address,
    nullif(try_cast(latitude as double), -999) as latitude,
    nullif(try_cast(longitude as double), -999) as longitude,
    raw_sha256,
    source_version,
    ingested_at
from {{ source('gios_air_quality', 'gios_air_archive_stations') }}
