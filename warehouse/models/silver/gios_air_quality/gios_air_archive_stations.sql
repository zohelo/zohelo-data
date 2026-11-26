-- Silver: GIOŚ air-quality monitoring stations of the archive's metadata workbook ("Metadane oraz kody stacji i
-- stanowisk pomiarowych", sheet STACJE; register PL-ENV-010, #3119): every station since 2000, closed ones too.
-- Grain: station_code (the current code); the Bronze table holds the newest version of the workbook only. The
-- workbook may list a station on two rows that differ only in old_station_codes (OpKrap3Maja in Nov 2026, Dev probe
-- 45756304318): the rows are joined, their old codes listed together; rows of one code that differ in any other
-- attribute fail the build (no row is picked). Source: bronze.gios_air_archive_stations. old_station_codes: the
-- codes the station had before ("o ile inny od aktualnego"), under which the older year files list it. Dates are
-- Excel serial day numbers in the workbook (1899-12-30 = 0); international code (EoI), type, area type, voivodeship
-- (upper case), locality and WGS84 coordinates as published (-999, the workbook's mark for an unknown coordinate,
-- is null).
with typed as (
    select
        station_code,
        international_code,
        station_name,
        nullif(trim(old_station_codes), '') as old_station_codes,
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
),

joined as (
    select
        station_code,
        count(
            distinct row(
                international_code, station_name, opened_on, closed_on, station_type, area_type, station_kind,
                voivodeship, locality, address, latitude, longitude
            )
        ) as variants,
        min(international_code) as international_code,
        min(station_name) as station_name,
        string_agg(distinct old_station_codes, ', ' order by old_station_codes) as old_station_codes,
        min(opened_on) as opened_on,
        min(closed_on) as closed_on,
        min(station_type) as station_type,
        min(area_type) as area_type,
        min(station_kind) as station_kind,
        min(voivodeship) as voivodeship,
        min(locality) as locality,
        min(address) as address,
        min(latitude) as latitude,
        min(longitude) as longitude,
        count(*) as workbook_rows,
        min(raw_sha256) as raw_sha256,
        min(source_version) as source_version,
        min(ingested_at) as ingested_at
    from typed
    group by station_code
)

select
    station_code,
    international_code,
    station_name,
    old_station_codes,
    opened_on,
    closed_on,
    station_type,
    area_type,
    station_kind,
    case
        when variants > 1
            then error('gios_air_archive_stations: ' || station_code || ' on rows that differ beyond the old codes')
        else voivodeship
    end as voivodeship,
    locality,
    address,
    latitude,
    longitude,
    workbook_rows,
    raw_sha256,
    source_version,
    ingested_at
from joined
