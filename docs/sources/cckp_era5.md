# World Bank CCKP ERA5 (`cckp_era5`)

**Built (26 Nov 2026, register GL-036 Copernicus Climate Data Store, #2421).** The Copernicus Climate Data Store
serves ERA5 as gridded fields only; the World Bank Climate Change Knowledge Portal (CCKP,
https://climateknowledgeportal.worldbank.org/download-data) aggregates ERA5 0.25-degree reanalysis over the land of
each country and territory and of their first-level units and publishes the series through an API. That is the route
the register chose.

Route (`ingestion/to_landing/cckp_era5/`): the documented download API
`https://datacatalogapi.worldbank.org/ddhxext/v3/cckp/<dataset id>/<geocodes>?_format=json`, dataset id
`era5-x0.25_timeseries_<var>_timeseries_<agg>_<first>-<last>_mean_historical_era5_x0.25_mean`. The same ids answer on
cckpapi.worldbank.org, but its robots.txt answers HTTP 500, which RFC 9309 reads as a full disallow; the datacatalogapi
host's robots.txt answers 404 (no rules) and is read before every run. Each read: the period, the newest `1950-<last>`
from the current year down to 2024 whose annual tas and pr answers for POL are not empty (the World Bank publishes a
new id when it extends the series: 1950-2024, then 1950-2025; 1950-2026 answered empty on 26 Nov 2026); then tas (mean
air temperature) and pr (precipitation), monthly and annual, for `all_countries` (246 areas) and for Poland's 16
first-level units in one request (codes comma-separated). About 11 requests, one at a time 5 s apart, retries with
backoff on 429 and 5xx, Retry-After honoured. Entity `era5` = the whole selection as one ZIP of the exact answers;
version = `<period>_<read started>` (no update time is published); a read equal to the newest copy writes nothing;
restart unit: the whole read (a read that fails part-way lands nothing). Historical reanalysis only: CMIP6 projections
are not observations and are never requested.

Voivodeship codes: `POL.1982370` … `POL.1982385`, from CCKP's spatial unit list `geonames.json` (download page,
"Spatial Unit's Names and Codes"), read in Dev probe 45810479823 and trial 45826698285. CCKP spells some names its own
way (`Lubeiskie`, `Malopolske`), so nothing is matched by name: the codes and their TERC codes are a stated list
(`poc_cckp_era5_voivodeships()` in `warehouse/macros/sources/cckp_era5.sql`, `POL_ADM1` in the extractor). The list is
not re-read at run time, because its host's robots.txt answered HTTP 403 to two of three Dev runners (a refusal, not
worked around). Instead each read requests exactly these codes and fails when an answer lacks one, and the test
`cckp_era5_observations_complete` fails on any first-level code outside the list.

Bronze `bronze.cckp_era5_records` (one row per variable x aggregation x geocode x period, all text). Silver
`silver.cckp_era5_observations` (variable x aggregation x geocode x period; tas in degC, pr in mm; annual pr is the
year's total, the sum of the months; annual tas is CCKP's yearly mean, within 0.15 °C of the plain average of the
months; annual periods are published as `YYYY-07`; a value that is not a number keeps `value` null, never 0; CCKP
prints no markers and leaves no gaps). Size (Dev trial 45831607817, 26 Nov 2026): one Landing file of 2.28 MB,
517,712 values, 1950-01 to 2025-12, 246 countries and territories and 16 voivodeships. Gold block `era5`: four measures
with one frequency each, `cckp_era5:era5:degC:tas.monthly` (M), `…:tas.annual` (A), `cckp_era5:era5:mm:pr.monthly` (M),
`…:pr.annual` (A). Countries are matched by ISO3 through `dim_geography`. Kosovo is CCKP `KSV` = World Bank `XKX`. 30
codes (29 territories and Taiwan) have no Gold area and stay `_X` with their code (59,280 rows). Voivodeships go to
`TERC:<code>` by the stated list. Values and units as published (D33). These are reanalysis estimates, not station
observations: IMGW (`imgw_archive`) stays the national observation source, and the two are kept side by side, never
merged.

Not linked to KPIs yet: the monthly measures fit the draft KPIs `air_temperature` and `precipitation` (O31), but their
metrics models and ceilings are declared by `imgw_archive` for 17 areas, and all CCKP countries would add about
225,000 rows to each. That needs the metrics models moved to `warehouse.yaml` as models of several sources, with a new
ceiling, first. Left out: the other CCKP variables (tasmax, tasmin, extremes), the seasonal aggregation, climatologies
and anomalies, other countries' first-level units, watersheds and EEZs, and the gridded NetCDF files.

Schedule: monthly, the first Thursday at 09:35 UTC (CCKP extends the series about once a year; O46 allows weekly at
most). Licence: CCKP's Terms of Use (download page) apply the World Bank Dataset Terms: CC BY 4.0 "unless specifically
labeled otherwise", plus a mandatory dispute procedure. The attribution format is "The World Bank: Dataset name: Data
source", used as "The World Bank: Climate Change Knowledge Portal, Observed Climate Data ERA5 0.25 degree: Copernicus
Climate Change Service (C3S) ERA5" (`cc_by_4_0_cckp_era5`).
