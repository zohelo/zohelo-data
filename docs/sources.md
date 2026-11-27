# Sources: download and discovery routes

All candidate sources (about 200) are listed in the source register
`ingestion/register/`: one file per source, `<id lower>.yaml` (D42), with the same fields (provider, access
kind, endpoint, interfaces, auth, limits, licence status, update behaviour, history and
incremental load approach, identifiers, research priority and blockers) and a `status`
(table below). A new entry is a new file; a review or build edits only its own entry's file. `family`,
`short_name` and `kpi` follow `.github/backlog/assets/register-fields.md`. The id prefix (GL, PLS, PL-ENV, PLG,
CD, EX, TP) groups the entries. An onboarded source links to its declaration
via `source_system`. The route of each onboarded source is its own file
[`docs/sources/<source_system>.md`](sources/) ([source-registration.md](source-registration.md)); this page takes
no sections of single sources.

| status | meaning |
|---|---|
| registered | researched |
| reviewing | stage 1 in progress |
| approved | cleared to build |
| built | code merged (Landing to Silver, schedule); production runs not yet proven |
| landing | stage reached |
| bronze | stage reached |
| silver | stage reached and green: set at Verify, after the scheduler's own runs (docs/playbook.md) |
| gold | in Gold: set at Verify, after a scheduled build |
| semantic | stage reached |
| published | in the portals |
| done | stage 11 (KPI sources) or 9 (reference sources) complete |
| parked | stopped, reason in the entry |

Researched download and discovery routes per source system, checked 8 September
2026. Each source's chosen extraction is recorded in `ingestion/sources/<source>.yaml`
([landing.md](landing.md) gives the decision order). A source without a file in `docs/sources/` has no
researched route yet.

## Implementation route matrix

The first five sources, side by side (each one's file has the details):

| Source | Complete discovery authority | Data route | Unit of restart |
| --- | --- | --- | --- |
| World Bank WDI | [WDI Data Catalog record](https://datacatalog.worldbank.org/search/dataset/0037712/world-development-indicators) | [`WDI_CSV.zip`](https://databank.worldbank.org/data/download/WDI_CSV.zip), whose current official redirect target is `https://databankfiles.worldbank.org/public/ddpext_download/WDI_CSV.zip` | whole ZIP, or byte offset only after a live `Range: bytes=0-0` probe returns `206` |
| Eurostat | [`inventory?type=data&lang=en`](https://ec.europa.eu/eurostat/api/dissemination/files/inventory?type=data&lang=en) | use each inventory row's exact TSV/CSV/SDMX URL; canonical full TSV pattern is `https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/{CODE}/1.0?format=tsv` | one dataflow; async key if returned; constrained partitions only after `413` |
| GUS BDL | paginated `/variables` plus subject, unit, locality and dictionary resources at `https://bdl.stat.gov.pl/api/v1` | `/data/by-variable/{variable-id}` with no `year` filter, page through all result units | one API page |
| NBP public statistics | the Statistics and Payment System navigation pages and every official downloadable attachment they link | API for exchange rates/gold; page-linked XLS/XLSX/CSV/XML/ZIP/PDF distributions for the rest | one API date window or one attachment |
| GUS DBW | indicator tree `https://dbw.stat.gov.pl/api_app/wsk/getIndicatorsTree` and each indicator's `api_app/getAggregatesById?id={id}&czy_pl=true` (bulk files with last-update date) | `https://dbw.stat.gov.pl/bulk_new/{file}.zip`, one semicolon CSV per indicator × cross-section × presentation × periodicity | one bulk file |

HTTP byte-range support is not promised in the reviewed provider documentation for
any of these routes. A downloader may use ranges only after verifying `206 Partial
Content` and a stable validator for that exact object. The portable recovery unit is
the ZIP, Eurostat dataflow/partition, BDL page, or NBP attachment shown above.
