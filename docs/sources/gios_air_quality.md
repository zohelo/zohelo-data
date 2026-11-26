# GIOŚ air quality (`gios_air_quality`)

**Built (register PL-ENV-010, #2960).** Route: the API v1 of the portal Jakość Powietrza,
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
`air_pm25_daily`, `air_no2_daily`, `air_o3_daily`, `air_so2_daily` (draft, no v2 map row yet). Schedule: daily 02:40 UTC.
Licence: CC BY 4.0 under the portal's Regulamin (`cc_by_4_0_gios_air_quality`), attribution "Źródło danych: GIOŚ -
EKOINFONET" with the time the data were produced and a note on processing.

**History:** the API keeps three days, so the series starts with the first scheduled pass and grows by a day a day.

**Left out:** verified history (follow-up #3119). The archive page https://powietrze.gios.gov.pl/pjp/archives lists
yearly ZIP files of verified measurements ("Wyniki pomiarów z <year> roku", `/pjp/archives/downloadFile/<id>`; the job
log of Dev probe 43322192937 shows 2022 to 2025, ids 564, 582, 644); none was downloaded, so their format and size are
unverified. The API v1 also lists `archivalData` services (`getDataBySensor/{idSensor}`,
`getDataForAllStationsByYearAndVoivodeship`, 2 requests a minute; untested). Verified values will get their own status
and tables, never mixed into the provisional series. Also not landed: the manual sensors (above), the air quality index
(`aqindex/getIndex`, derived from the same values), station metadata with type and opening dates (`metadata/stations`,
2 requests a minute), exceedance information, PM10 aggregates (used once by the Dev trial as a cross-check),
concentration maps and annual statistics.
