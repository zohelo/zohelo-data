# GIOŚ air quality (`gios_air_quality`)

**Built (register PL-ENV-010, #2960; verified history #3119).** Current data: the API v1 of the portal Jakość Powietrza,
https://api.gios.gov.pl/pjp-api/v1/rest/ (JSON-LD; OpenAPI at `/pjp-api/v3/api-docs`; the old `/pjp-api/rest/` services
were withdrawn on 30 June 2025). One daily pass, one request at a time: `station/findAll?size=500` (288 stations in one
page; further pages 31 s apart, the list endpoints are limited to 2 requests a minute), `station/sensors/{id}` per station
and `data/getData/{sensorId}?size=500` per sensor (the last full hour back to the third day, local time, µg/m³; stated
limit 1,500 a minute), 1 s apart: 2,164 requests (288 stations, 1,875 sensors), about 50 minutes; the Regulamin asks for
API downloads "nie częściej niż dwa razy na godzinę". Manual sensors (806 on 12 Nov 2026: BaP, metals and ions in dust,
deposition, part of PM10 and PM2.5) answer 400 ("Dla stanowiska typu manualnego wyniki pomiarów nie są dostępne na
bieżąco"; GIOŚ publishes them after 4 to 8 weeks through the archive services): they are recorded in the receipt
(`sensors_manual`), not loaded. Entity = one pass `pass_<YYYYMMDD>` (UTC day of the run), a ZIP of
the exact answers (station list, sensor lists, values); version = download time; one pass per day, so a rerun the same day
writes nothing; restart unit = the pass (`ingestion/to_landing/gios_air_quality/`).

Bronze (one Parquet file per pass and table, values as text): `bronze.gios_air_stations`, `bronze.gios_air_sensors`,
`bronze.gios_air_measurements`; an unknown member, field or value type, or a value older than five days before the pass
rejects the pass into `bronze.gios_air_quality_rejected_files`. Silver: `silver.gios_air_stations` and
`silver.gios_air_sensors` (dbt; newest pass that listed the station or sensor: id, code, name, WGS84 coordinates, city,
commune, district and voivodeship names as published; the API gives no TERYT codes; parameter code, formula and name) and
`silver.gios_air_hourly` (entity table, one file per measurement day, `ingestion/to_bronze/gios_air_quality/entities.py`
with `warehouse/entity/gios_air_hourly.sql`): sensor × hour, the newest pass that holds the hour wins, a null value stays
null, `validation_status = provisional` (GIOŚ: "Udostępniane dane nie są na bieżąco weryfikowane").

Gold block `air_daily` (`poc_build_gios_air_quality`, frequency D): daily mean of PM10, PM2.5, NO2, O3 and SO2 for
Poland and the 16 voivodeships; a sensor-day counts with at least 18 of 24 hourly values (75 % data capture), the area
value is the mean of its stations' sensor-day means, voivodeship = the station's own `Województwo` field mapped by a
static list to TERC (`warehouse/macros/sources/gios_air_quality.sql`). Status `provisional`. KPIs `air_pm10_daily`,
`air_pm25_daily`, `air_no2_daily`, `air_o3_daily`, `air_so2_daily` (draft, no v2 map row yet). Schedule: Landing daily
02:40 UTC (the API keeps three days), Bronze and Silver with the weekly full run, Saturday 03:10 UTC (O46).
Licence: CC BY 4.0 under the portal's Regulamin (`cc_by_4_0_gios_air_quality`), attribution "Źródło danych: GIOŚ -
EKOINFONET" with the time the data were produced and a note on processing.

**History:** the API keeps three days, so the provisional series starts with the first scheduled pass and grows by a
day a day. The verified series below start in 2000.

## Verified history: the yearly archive (#3119)

Route: the bulk feature of the portal, the archive page https://powietrze.gios.gov.pl/pjp/archives, whose files are plain
links `/pjp/archives/downloadFile/<id>` (no robots.txt: HTTP 404; the page's reCAPTCHA guards only its own query export,
`/archives/data/getFileForSelectedStations`, not these files; Dev probes 45746079470, 45747293591, 45750149567).
"Wyniki pomiarów z <year> roku", 2000 to 2025: one ZIP per year (2.3 MB for 2000, 26 to 76 MB since 2005; the server
sends Content-Length only, no Last-Modified or ETag, and once cut a 75 MB download short, so a short body is retried),
plus "Metadane oraz kody stacji i stanowisk pomiarowych" (one workbook, sheet STACJE: 1,148 rows of stations since 2000
with their old codes, voivodeship and WGS84). "Statystyki z lat 2000-2025" is not loaded. GIOŚ replaces a file under a
new id (2016 and 2018 have ids 602 and 603 among 2xx and 3xx neighbours), so the id is the version: an entity is landed
when the page lists an id its newest receipt does not name. Entity = one year `archive_<year>` (.zip) or
`archive_stations` (.xlsx); version = download time; one request at a time, 5 s apart, bounded retries; station file
first, then missing years newest first; resumable within the time budget (`ingestion/to_landing/gios_air_quality/archive.py`,
run by `landing.extract` after the day's pass). The API v1 `archivalData` services (2 requests a minute) are not used.

Each year's ZIP holds one workbook per indicator and averaging time (`2025_PM10_1g.xlsx`, `2013_PM2.5_24g.xlsx`; ions and
ozone precursors with several indicators in one workbook). The first sheet is a wide table: a header block labelled in
column A (`Kod stacji`, `Wskaźnik`, `Czas uśredniania`, and by year `Nr`/`Numer`, `Jednostka`, `Kod stanowiska`, `Czas
pomiaru`, `Rok`, or a row without a label), one column per station and indicator, one row per period (Excel serial
number: hourly values stamped at the end of the hour, 01:00 to 24:00; daily values at midnight), values as numbers or,
2016 to 2018, as text with a decimal comma. Bronze (`bronze.gios_air_archive_values`, entity table, one file per year,
`ingestion/to_bronze/gios_air_quality/archive.py` with DuckDB's `read_xlsx`): one row per non-empty value cell with the
column's header cells as published, member, sheet row and column; an unknown label, a header block without `Kod stacji`,
a value in a column without a station code, or a period that is not a number or lies more than five minutes off a whole
hour rejects the year into `bronze.gios_air_quality_rejected_files`. Not loaded and listed in the run annotation: the
deposition workbooks (monthly, two date columns) and the further sheets of a workbook (old station codes; the 2015, 2016
and 2018 ion workbooks also split their ions by ion or station over further sheets, of which the first sheet is loaded).
`bronze.gios_air_archive_stations`: the station sheet, header matched by name.

Silver: `silver.gios_air_archive_values` (entity table, one file per year, `archive_load.py` with
`warehouse/entity/gios_air_archive_values.sql`): grain station code × indicator × averaging time × stamp × member (two
workbooks may publish one series: benzene at KpZielBoryTu in both the C6H6 and the ozone-precursor workbook of 2024 and
2025; both rows are kept), `validation_status = verified`, never mixed with the provisional values. Indicator = the
`Wskaźnik` cell, else the member name; averaging time = the header code (`1g`, `24g`, `1t`: 71 benzene cells of 2013),
else the member name (the header also prints `SO2-24g`, `(PM2.5)-24g`, station codes), else the period grid, and it must
agree with the grid (hours or whole days) or the year is rejected; a few columns of 2002 to 2009 carry garbled header
cells (indicator `awWSSE` or `Arch`, 51 to 208 cells a year), kept as published; stamp rounded to the hour (the serials drift by up to a minute in a year); `measured_on` = the
day the value describes (hour stamp minus one hour). Values as published: zeros and small negative hourly values exist
(instrument noise near zero) and are kept; no text markers occur, and a cell that is not a number would reject its year.
`silver.gios_air_archive_stations` (dbt): one row per station code (a station on two rows that differ only in old codes
is joined), dates, type, voivodeship, WGS84 (-999 = unknown → null). Restart unit: the year file; a new file version
replaces that year in Bronze and Silver only (`refresh` unit "year file").

Gold blocks `air_verified_daily` (D, Poland) and `air_verified_monthly` (M, Poland and the 16 voivodeships) of
`poc_build_gios_air_quality`, from `poc_gios_air_verified` (read through `poc_optional_ref`, so the provisional block
never waits for the archive): automatic hourly values of PM10, PM2.5, NO2, O3 and SO2 (µg/m³; any other unit fails the
build); a station-day counts with at least 18 of 24 hourly values (the provisional rule), a station-month with at least
75 % of its days; Poland needs at least 10 stations, a voivodeship one. Voivodeship = the station workbook's
`Województwo`, found by the year file's station code or one of the station's old codes (an old code given to stations of
two voivodeships fails the build; codes found nowhere, a few mobile stations, count for Poland only). Own measure_keys
(`gios_air_quality:air_verified_daily:ug_m3:<code>`, `…:air_verified_monthly:…`), status `ok`. KPIs
`air_<pollutant>_daily_verified` and `air_<pollutant>_monthly_verified` (draft). Not in Gold: manual daily values
(`24g`), other indicators (CO, benzene, metals, PAHs, ions, NOx, NO), and a daily series for the voivodeships.

Trial (Dev run TRIAL_RUN, final code): TRIAL_NUMBERS

**Left out:** the manual sensors of the API (above; their values are in the archive), the air quality index
(`aqindex/getIndex`, derived from the same values), API station metadata (`metadata/stations`), exceedance information,
PM10 aggregates, concentration maps, the archive's annual statistics workbook and deposition workbooks.
