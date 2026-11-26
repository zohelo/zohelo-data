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
host's robots.txt answers 404 (no rules). Each read: CCKP's `geonames.json` (spatial unit names and codes); the period,
the newest `1950-<last>` from the current year down to 2024 whose annual tas and pr answers for POL are not empty (the
World Bank publishes a new id when it extends the series: 1950-2024 then 1950-2025; 1950-2026 answered empty on
26 Nov 2026); then tas (mean air temperature) and pr (precipitation), monthly and annual, for `all_countries` (246
areas) and for Poland's 16 first-level units in one request (codes from `geonames.json`, comma-separated). About 12
requests, one at a time 5 s apart, retries with backoff on 429 and 5xx, Retry-After honoured. Entity `era5` = the
whole selection as one ZIP of the exact answers; version = `<period>_<read started>` (no update time is published);
a read equal to the newest copy writes nothing; restart unit: the whole read (a read that fails part-way lands
nothing). Historical reanalysis only: CMIP6 projections are not observations and are never requested.

Bronze `bronze.cckp_era5_records` (`unit` rows from `geonames.json`, `value` rows per variable x aggregation x geocode
x period, all text). Silver `silver.cckp_era5_units` (geocode, name as published, parent country) and
`silver.cckp_era5_observations` (variable x aggregation x geocode x period; tas in degC, pr in mm; annual pr is the
year's total, the sum of the months; annual periods are published as `YYYY-07`; a value that is not a number keeps
`value` null, never 0). Gold block `era5` (`warehouse/macros/sources/cckp_era5.sql`): four measures with one frequency
each, `cckp_era5:era5:degC:tas.monthly` (M), `…:tas.annual` (A), `cckp_era5:era5:mm:pr.monthly` (M), `…:pr.annual` (A);
countries by ISO3 through `dim_geography`, voivodeships by a stated code map (`POL.1982370` Dolnośląskie … `POL.1982385`
Zachodniopomorskie → `TERC:<code>`). CCKP spells some names its own way (`Lubeiskie`, `Malopolske`), so nothing is
matched by name: the test `cckp_era5_units_complete` fails when a mapped code is missing or renamed. Values and units
as published (D33). These are reanalysis estimates, not station observations: IMGW (`imgw_archive`) stays the national
observation source, and the two are kept side by side, never merged.

Not linked to KPIs yet: the monthly measures fit the draft KPIs `air_temperature` and `precipitation` (O31), but their
metrics models and ceilings are declared by `imgw_archive` for 17 areas, and all CCKP countries would add about
225,000 rows to each; that needs the metrics models moved to `warehouse.yaml` as models of several sources with a new
ceiling first. Left out: the other CCKP variables (tasmax, tasmin, extremes), the seasonal aggregation, climatologies
and anomalies, other countries' first-level units, watersheds and EEZs, and the gridded NetCDF files.

Schedule: monthly, the first Thursday at 09:35 UTC (CCKP extends the series about once a year; O46 allows weekly at
most). Licence: CCKP's Terms of Use (download page) apply the World Bank Dataset Terms: CC BY 4.0 "unless specifically
labeled otherwise", plus a mandatory dispute procedure; attribution "The World Bank: Dataset name: Data source", used
as "The World Bank: Climate Change Knowledge Portal, Observed Climate Data ERA5 0.25 degree: Copernicus Climate Change
Service (C3S) ERA5" (`cc_by_4_0_cckp_era5`).
