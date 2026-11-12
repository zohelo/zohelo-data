# GIOŚ air quality (`gios_air_quality`)

**Built (register PL-ENV-010, #2960).** Route: the API v1 of the portal Jakość Powietrza,
https://api.gios.gov.pl/pjp-api/v1/rest/ (JSON-LD; OpenAPI at `/pjp-api/v3/api-docs`; the old `/pjp-api/rest/` services
were withdrawn on 30 June 2025). One daily pass, one request at a time: `station/findAll?size=500` (288 stations in one
page; further pages 31 s apart, the list endpoints are limited to 2 requests a minute), `station/sensors/{id}` per station
and `data/getData/{sensorId}?size=500` per sensor (the last full hour back to the third day, local time, µg/m³; stated
limit 1,500 a minute), 1 s apart: about 1,400 requests. Entity = one pass `pass_<YYYYMMDD>` (UTC day of the run), a ZIP of
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
`air_pm25_daily`, `air_no2_daily`, `air_o3_daily`, `air_so2_daily` (draft, no v2 map row yet). Schedule: daily 02:40 UTC.
Licence: CC BY 4.0 under the portal's Regulamin (`cc_by_4_0_gios_air_quality`), attribution "Źródło danych: GIOŚ -
EKOINFONET" with the time the data were produced and a note on processing.

**Left out:** verified history. GIOŚ publishes annual ZIP files of verified measurements for 2000-2025
(https://powietrze.gios.gov.pl/pjp/archives, `/pjp/archives/downloadFile/<id>`, spreadsheets per pollutant and averaging
time) and API v1 `archivalData` endpoints (`getDataBySensor/{id}`, `getDataForAllStationsByYearAndVoivodeship`, 2 requests
a minute). Loading them needs a spreadsheet decoder per layout and an entity table per year (about 9 million hourly
values a year); it is a follow-up, and verified values will get their own status, never mixed into the provisional
series. Also not landed: the air quality index (`aqindex/getIndex`, derived from the same values), exceedance
information, PM10 aggregates, concentration maps and annual statistics.
