# IRENASTAT (`irena_stat`)

**Built (26 Nov 2026, register GL-035, #1268).** The International Renewable Energy Agency publishes its statistics
in IRENASTAT, a PxWeb database (https://pxweb.irena.org/pxweb/en/IRENASTAT) with an API and no bulk file. The API
is the route: `https://pxweb.irena.org/api/v1/en/IRENASTAT` lists folders and tables (GET) and answers data to POST
queries as JSON-stat 2.0. Its robots.txt answers 404 (no rules) and is read before every run. www.irena.org refuses
runners (robots.txt HTTP 403) and is not used.

Route (`ingestion/to_landing/irena_stat/`): the table `Country_ELECSTAT_<year>_H<n>_PX` of the folder "Power
Capacity and Generation". It holds 224 countries and areas (ISO3) x 26 technologies x 2 data types (Electrical
Installed Capacity (MW), Electricity Generation (GWh)) x 3 grid connections (All, On-grid, Off-grid) x 26 years,
908,544 cells in release 2026 H2 (updated 28 Sep 2026). The API states no cell limit (`?config` answers 404), so the
table is queried in parts of whole countries of at most 50,000 cells (19 queries). A part answered with HTTP 403 (how
PxWeb refuses a query above its limit) is halved, never below one country. Requests go one at a time, 2 s apart, with
retries and backoff on 429 and 5xx. Entity `country_elecstat` = the table without its release: one ZIP of the exact
answers (`metadata.json`, `data_<nnn>.json`). Version = `<release>_<updated>` from the listing (the JSON-stat answer
stamps 9999-12-31). An unchanged version is not read again (one listing request), and equal bytes write nothing.
Restart unit: the table (a read that fails part-way lands nothing).

Bronze `bronze.irena_stat_electricity`: one row per cell with a value or a status, holding PxWeb codes and labels.
IRENA's technology, data type, grid and year codes are positions in the release (`0` … `25`), so Silver keys on the
labels. A cell without a value marked `..` (PxWeb's "no data") is not stored: 718,615 of the 908,544 cells. A file
whose variables, codes, labels or countries differ from its metadata is rejected with the reason. Silver
`silver.irena_stat_electricity` (country x technology x data type x grid connection x year): measure `capacity` (MW,
a stock at the end of the year) or `generation` (GWh, a flow over the year). Values are as published: 2,555 published
zeros are kept, and a value never comes from a missing cell. Size (Dev trial 45868633684, 26 Nov 2026): one Landing
file of 2.15 MB, 189,929 rows, capacity 2000-2025 and generation 2000-2024. 'All' equals On-grid + Off-grid within 1 %
for every cell that has both.

Gold block `elecstat` (`poc_build_irena_stat`): one measure per data type and technology,
`irena_stat:elecstat:<MW|GWh>:<capacity|generation>.<technology>` (48 measures with values), annual. The grid
connection is the breakdown, and `All` is the total. Technology parents (`Total Renewable`, `Total Non-Renewable`,
`Renewable hydropower`) and their children are separate measures and are never summed or split here: IRENA's terms
forbid disaggregating its statistics. Countries are matched by ISO3 through `dim_geography` (208). 16 codes have no
Gold country and stay `_X` with their code (AIA, BES, BLM, COK, FLK, GLP, GUF, MSR, MTQ, MYT, NIU, REU, SGS, SPM, TKL,
TWN; 9,466 rows). Draft KPIs: `renewable_capacity` (Total Renewable capacity, All) and `renewable_generation` (Total
Renewable generation, All), with metrics models, value bounds and v2 map rows. Licence: `unresolved`, so the licence
gate keeps them internal.

Left out: the regional table (`Region_ELECSTAT`, IRENA's own regions with another technology grouping, no Gold
area) and the renewable-share table (`RE-SHARE`) of the same folder, and the folders "Heat Generation" and
"Finance". Eurostat energy statistics, World Bank WDI and UN energy statistics cover overlapping series. Each stays
its own source (O31).

Schedule: monthly, on the 11th at 07:25 UTC (IRENASTAT is updated twice a year; O46 allows weekly at most). Licence:
IRENA Terms and Conditions (https://www.irena.org/terms-and-conditions): "Users may freely access, copy, share,
download, reproduce, print and/or view IRENA's Content for non-commercial purposes, provided that appropriate
attribution is given to IRENA." and "No other use shall be made of IRENA's Content without IRENA's advanced written
permission."; 2.1(viii) forbids attempts "to disaggregate any data, material or statistics". Commercial reuse needs
written permission, so the source is `unresolved` (internal) until the owner's question in the register is answered.
Attribution: "Source: IRENA (<year>), IRENASTAT Online Data Query Tool, <download date>."
