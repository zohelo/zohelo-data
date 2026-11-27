# World Bank Enterprise Surveys (`world_bank_es`)

**Built (27 Nov 2026, register GL-010, #3315).** The World Bank Enterprise Surveys (https://www.enterprisesurveys.org/)
interview firms in about 180 economies, one economy at a time, and publish indicators by economy and survey year
(corruption, finance, infrastructure, regulation, competition, gender, innovation, performance). The World Bank
republishes those indicators as Data360 dataset `WB_ES`, and that copy is the route. The firm-level microdata (login
under the Data Access Protocol) are never used.

Route (bulk first; shared `bulk_file` pattern, no extractor code): the Data360 file folder holds the whole dataset in
one file, `https://data360files.worldbank.org/data360-data/data/WB_ES/WB_ES.csv` (606,309,542 bytes, Last-Modified
19 Nov 2026; Dev probe 45924752018), SDMX-CSV with labels, the same 41 columns as ITU_DH (GL-031): STRUCTURE …
OBS_CONF, then a `*_LABEL` column per code. Entity `wb_es` = the file; version = its Last-Modified; a copy lands only
when its SHA-256 changed. Restart unit: the file. The register's Data360 REST API (`/data360/data?DATABASE_ID=WB_ES`,
1,000 rows a page, about 1,200 pages) is not used: one request instead of 1,200, and its paging repeated rows in a full
read of ITU_DH.

Bronze `bronze.world_bank_es_observations`: one row per CSV row, all 41 columns as published text (names in lower
case), read by DuckDB; the bytes go to a temporary file as they are (checked as UTF-8 in slices, no decoded copy in
memory). A file with another header, a row of another width or no row is rejected with the reason
(`bronze.world_bank_es_rejected_files`). Silver `silver.world_bank_es_observations`: grain indicator x economy (ISO3)
x sex x age x urbanisation x sector x firm size x other firm characteristic x unit x survey year. Indicators
`WB_ES_T_*` (191) cover all firms; `WB_ES_M_*` (190) and `WB_ES_S_*` (180) were asked of manufacturing or services
firms only and have no all-firms total. Breakdowns as published: `COMP_BREAKDOWN_1` sector (67 codes: `_T`, major
sectors and detailed sectors), `COMP_BREAKDOWN_2` firm size (`WB_ES_SIZE_1` large 100+, `_2` medium 20-99, `_3` small
5-19), `COMP_BREAKDOWN_3` other characteristics (`WB_ES_OTHER_1` to `_9`: exporter and ownership type, …); `_T` total,
`_Z` not applicable; sex, age and urbanisation are `_Z` throughout. `TIME_PERIOD` is published as `2023.0`; `year`
is its integer. Size (Dev trial 45932158707, 27 Nov 2026): one Landing file of 606.3 MB, 1,195,422 rows (every row of
the file), 561 indicators, 181 economies, survey years 2006-2026, every row OBS_STATUS `A` (normal value), OBS_CONF
`PU`, UNIT_MULT `0`; no empty or non-numeric value; 74,860 published zeros kept.

Gold block `wb_es` (`poc_build_world_bank_es`), the smallest valuable selection: the all-firms indicators `WB_ES_T_*`
of every economy and survey year, totals only (sector, size and other characteristic all `_T`); one measure per
indicator, `world_bank_es:wb_es:<UNIT_MEASURE>:<INDICATOR>`, annual with `date_key` 1 January of the survey year;
values and units as published (D33; UNIT_MULT is 0 in every row). Units: `PT_F` % of firms, `PT` %, `D` days, `IX`
index (0 to 100), `SCORE` score (0 to 100), `COUNT` number, `YR` years, `H` hours, `WK` weeks, `M` months, each
mapped in `unit_map`. Economies by ISO3 through `dim_geography`. Trial: 70,029 rows (every all-firms total in Silver),
191 measures, 33,379 series, 0 duplicates by series_key x date_key; 180 economies matched, `TWN` (Taiwan, China; 190
rows) has no Gold country and stays `_X` with its code. KPIs (new drafts, with metrics models,
value bounds and v2 map rows): `firms_bribery_incidence` (WB_ES_T_GRAFT3, % of firms experiencing at least one bribe
payment request), `firms_finance_constraint` (WB_ES_T_FIN16, % of firms identifying access to finance as a major or
very severe constraint) and `operating_licence_days` (WB_ES_T_BUS2, days to obtain an operating license). Poland
(survey years 2009, 2013, 2019, 2025), 2025: bribery incidence 8.02 % of firms, access to finance a major constraint
24.82 %, 2.94 days to obtain an operating licence.

Left out of Gold (kept in Silver): the breakdowns of `WB_ES_T_*` by major sector, size, exporter and ownership type,
and the `WB_ES_M_*` / `WB_ES_S_*` indicators (manufacturing- or services-only, by detailed sector). The trial compared the 69,483 `WB_ES_M_*` rows for
all manufacturing (`WB_ES_M_SECTOR_1`) with `WB_ES_T_*` by major sector manufacturing: every value is equal, so these
indicators repeat the all-firms ones by sector.

Overlap: WDI (GL-005) repeats about 20 of these indicators (`IC.FRM.*`, `IC.TAX.*`); both are kept, each its own
source (O31).

Schedule: monthly, on the 24th at 06:25 UTC (no release calendar; economies are released one at a time; O46 allows
weekly at most). Silver is rebuilt in full when the file changed (D32), above the 1 M rows of #822: the exception is
recorded on #3315 (ceiling 1,800,000). Licence: `unresolved` (internal; the licence gate keeps it out of v1 and v2).
The register's reading (26 Nov 2026): the World Bank Dataset Terms
(https://www.worldbank.org/ext/en/legal/terms-conditions/datasets) say "Unless specifically labeled otherwise, these
Datasets are provided to you under a Creative Commons Attribution 4.0 International License (CC BY 4.0), with the
additional terms below.", and https://data.worldbank.org/summary-terms-of-use that some datasets "are provided by third
parties, and may not be redistributed or reused without the consent of the original data provider"; the Enterprise
Surveys page https://www.enterprisesurveys.org/en/citing-the-data restricts only the microdata. The WB_ES catalogue
label could not be read (HTTP 429), so the status stays unresolved until it is. Attribution if published: "Source:
World Bank Enterprise Surveys, www.enterprisesurveys.org".
