# Sources: download and discovery routes

All candidate sources (about 180) are listed in the source register
`ingestion/register.yaml`: one entry per source with the same fields (provider, access
kind, endpoint, interfaces, auth, limits, licence status, update behaviour, history and
incremental load approach, identifiers, research priority and blockers) and a `status`
(table below). The id prefix (GL, PLS, PL-ENV, PLG, CD, EX) groups the entries. An onboarded source links to its declaration
via `source_system`. This page details the routes of the onboarded sources.

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
([landing.md](landing.md) gives the decision order). Sources not listed here have no
researched route yet.

## Implementation route matrix

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

## World Bank World Development Indicators

The official WDI guide directs bulk users to the Data Catalog and describes bulk
files as data plus metadata. The catalogue currently describes over 1,500 indicators,
over 200 countries and territories, annual temporal coverage 1960–2025, and the CSV
ZIP as **269.7 MB**, last updated **17 July 2026**. Catalogue metadata itself was last
updated **20 July 2026**.

Use the stable publisher URL:

```text
https://databank.worldbank.org/data/download/WDI_CSV.zip
```

It currently redirects to the official download host:

```text
https://databankfiles.worldbank.org/public/ddpext_download/WDI_CSV.zip
```

For a reproducible bootstrap, the current dated object exposed by the catalogue is:

```text
https://datacatalogfiles.worldbank.org/ddh-published/0037712/DR0095335/WDI_CSV_2026_07_15.zip
```

The stable object changes when WDI is refreshed. Record the initial URL, final
redirect URL, response `ETag` and `Last-Modified` when present, byte count, retrieval
time, and SHA-256. Stream to a temporary file and atomically publish only after ZIP
integrity and every member have been checked. The catalogue does not publish a
normative member manifest, so enumerate and retain every ZIP member. In particular,
identify the observation file and country, series, country-series, series-time,
and footnote metadata by their headers; do not silently accept a data-only archive.

**Implemented:** the shared `bulk_file` pattern (`ingestion/to_landing/patterns/`,
declared in `world_bank_wdi.yaml`) lands the stable-URL archive as
entity `WDI_CSV` (version = Last-Modified; written only when the SHA-256 changed, after
the ZIP CRC check); Bronze `bronze.wdi_bulk_observations` (long: country × indicator ×
year with a value) and `bronze.wdi_bulk_metadata` (other members as field/value rows,
members identified by header). The legacy content-addressed objects stay until the new
Bronze is verified.

The catalogue marks this WDI dataset Public and [Creative Commons Attribution 4.0](https://datacatalog.worldbank.org/public-licenses?fragment=cc).
CC BY 4.0 permits sharing and adaptation, including commercial use, with attribution
and an indication of changes. No WDI-specific geography restriction was found.

Primary evidence:

- [WDI user guide: bulk downloads and archived versions](https://datatopics.worldbank.org/world-development-indicators/user-guide.html)
- [WDI Data Catalog: current resources, size, timestamps, coverage and licence](https://datacatalog.worldbank.org/search/dataset/0037712/world-development-indicators)
- [World Bank public licences](https://datacatalog.worldbank.org/public-licenses?fragment=cc)

## Eurostat

There is no single current archive for all ordinary Eurostat datasets. The supported
complete route is catalogue enumeration followed by one prepared, compressed full
file per inventory item. Eurostat explicitly warns that iterating the inventory can
start downloading the complete Eurostat data scope and require substantial disk.

### Complete discovery and metadata

```text
# Exact inventory intended for download scripts; TSV text
https://ec.europa.eu/eurostat/api/dissemination/files/inventory?type=data&lang=en

# All dataflow stubs, SDMX 2.1
https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/1.0?detail=allstubs

# All dataflows, SDMX 3.0
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/structure/dataflow/ESTAT/*

# All current structural families, SDMX 3.0
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/structure/codelist/ESTAT/*
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/structure/conceptscheme/ESTAT/*
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/structure/datastructure/ESTAT/*

# Complete catalogue and changes since the previous catalogue update
https://ec.europa.eu/eurostat/api/dissemination/catalogue/dcat/ESTAT/FULL
https://ec.europa.eu/eurostat/api/dissemination/catalogue/dcat/ESTAT/UPDATES

# Search/navigation table and series-to-dimension-position metabase
https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=en
https://ec.europa.eu/eurostat/api/dissemination/catalogue/metabase.txt.gz
```

The inventory supplies each item's exact data download URLs and `Last data change`;
use those fields as the download authority. A dataflow also provides `OBS_COUNT`,
oldest/latest observation periods, `UPDATE_STRUCTURE`, `UPDATE_DATA`, the referenced
versioned DSD, and metadata links. Eurostat checks for dataset catalogue changes at
11:00 and 23:00 CET. Dataflow and data-constraint version `1.0` must be re-fetched;
DSDs, concept schemes and codelists use their referenced immutable version.

### Full data files and structures

Use the exact URL from the inventory. Equivalent full-dataset forms are:

```text
# TSV, compression on by default in SDMX 3.0
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/{CODE}/1.0?format=tsv

# SDMX-CSV 2.0
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/{CODE}/1.0?format=csvdata&formatVersion=2.0

# Older API-compatible gzip TSV
https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{CODE}?format=TSV&compressed=true

# Dataset-specific allowed positions and referenced DSD
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/structure/dataconstraint/ESTAT/{CODE}/1.0
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/structure/dataflow/ESTAT/{CODE}/1.0
```

Eurostat says the inventory-linked full files are prepared in advance. An exact
cached request is returned synchronously before extraction-cost thresholds are
applied, so the full inventory TSV normally arrives as one compressed response even
when the theoretical cube exceeds five million cells. Validate the response body:
Eurostat says a prepared URL can rarely return an async XML envelope instead.

For an uncached extraction, the documented decision is:

- under 500,000 estimated cells: synchronous;
- 500,000 through 5,000,000: asynchronous;
- above 5,000,000: HTTP 413 and the query must be partitioned using the data constraint.

The async flow is:

```text
GET the original SDMX data URL -> XML containing queued id
GET https://ec.europa.eu/eurostat/api/dissemination/1.0/async/status/{id}
GET https://ec.europa.eu/eurostat/api/dissemination/1.0/async/data/{id}
```

Poll at a bounded interval while status is `SUBMITTED` or `PROCESSING`; fetch only
when `AVAILABLE`. `EXPIRED` means submit again; Eurostat says expiry happens after a
few days or when the dataset is updated. The provider recommends one extraction at a
time, no parallel extraction script, and bulk download where applicable.

Comext `DS-*` trade cubes use the separate
`https://ec.europa.eu/eurostat/api/comext/dissemination/...` base and can be far above
the extraction ceiling. Prefer Eurostat's prepacked Comext files where they cover the
required distribution:

```text
# Recursive official inventory with raw byte sizes and ISO Last-Modified values
https://ec.europa.eu/eurostat/api/dissemination/files?format=csv&dir=comext&hierarchy=true&sizeFormat=NONE&dateFormat=ISO

# Download one inventory path
https://ec.europa.eu/eurostat/api/dissemination/files?file=comext/{NAME}
```

Otherwise obtain the Comext content constraint and partition the query so every
uncached request stays at or below five million estimated cells. Preserve all
dimensions, time positions and observation flags across the partition union.

Eurostat generally authorises commercial and non-commercial reuse with source
acknowledgement. Therefore downloading all available geographies into a private,
non-commercial collection is compatible with the notice. A commercial publication
must enforce the stated exceptions: remove data for countries outside EU, EFTA and
official EU acceding/candidate countries; exclude restricted third-party material;
exclude Switzerland/Liechtenstein declaring-country trade from 1995 for HS, SITC,
BEC, NSTR and national classifications; and exclude Austrian declaring-country CN8
trade. Retain per-item copyright notices because they can add conditions.

Primary evidence:

- [Periodic download example and complete inventory](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-faq/examples)
- [SDMX 3.0 structures, complete datasets, formats and compression](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-getting-started/sdmx3.0)
- [Async thresholds, lifecycle and fair use](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/asynchronous-api)
- [Migration guide, inventories, metabase and Comext file catalogue](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-migrating/bulkdownload)
- [DCAT full catalogue and updates](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/catalogue-api/dcat)
- [Eurostat reuse notice and geography/trade exceptions](https://ec.europa.eu/eurostat/web/main/help/copyright-notice)

## GUS Local Data Bank (BDL)

**Extraction (owner decision 13 Oct 2026).** Entity = subgroup (P####, an API `subject`
with variables); version = its `lastUpdate` (`/subjects/{id}`, Warsaw local time). The
initial load is the website's subgroup export ("CSV – tablica relacyjna" ZIP; large
subgroups in parts), one browser and one route, no IP rotation. Changed subgroups are
refreshed whole through the API with the registered key, so every Landing version is a
complete subgroup and the newest version wins, whichever channel produced it. Website
CSV columns = `Kod;Nazwa;<subgroup dimensions>;Rok;Wartosc;Jednostka miary;Atrybut`; the
dimension values equal the API variables' name components n1…n5, which is how Silver
maps website rows to variable ids.

The reviewed [BDL v1 OpenAPI](https://bdl.stat.gov.pl/api/v1/swagger/doc/swagger.json)
has no bulk export and no changed-variable feed. Complete collection must enumerate
the API. The public API exposes the same statistical scope as the BDL application,
including annual and short-period statistics from 1995 onward.

```text
BASE=https://bdl.stat.gov.pl/api/v1

# Variables: zero-based pages, stable source-ID ordering
$BASE/variables?lang=pl&page={PAGE}&page-size={PAGE_SIZE}&sort=id
$BASE/variables/{VARIABLE_ID}?lang=en

# All observations for a variable: omit year and exhaust pages
$BASE/data/by-variable/{VARIABLE_ID}?lang=en&page={PAGE}&page-size={PAGE_SIZE}

# Metadata catalogues and details, in pl and en where labels are translated
$BASE/subjects?lang={LANG}&page={PAGE}&page-size={PAGE_SIZE}&sort=id
$BASE/subjects/{SUBJECT_ID}?lang={LANG}
$BASE/units?lang={LANG}&page={PAGE}&page-size={PAGE_SIZE}&sort=id
$BASE/units/{UNIT_ID}?lang={LANG}
$BASE/units/localities?parent-id={12_CHARACTER_MUNICIPALITY_ID}&lang={LANG}&page={PAGE}&page-size={PAGE_SIZE}
$BASE/units/localities/{LOCALITY_ID}?lang={LANG}
$BASE/attributes?lang={LANG}
$BASE/aggregates?lang={LANG}
$BASE/levels?lang={LANG}
$BASE/measures?lang={LANG}
$BASE/years?lang={LANG}
```

The live catalogue reported **172,573 variables** and **4,694 ordinary units** on
8 September 2026. Localities have no valid root listing: enumerate ordinary units,
then call `/units/localities` for every level-6 municipality using its 12-character
BDL ID. Preserve leading zeroes in unit IDs. Fetch Polish and English metadata, and
retain `lastUpdate`, descriptions, dimensions, units, aggregates, attributes,
precision and formatted values.

The official anonymous limits are 5 requests/second, 100/15 minutes, 1,000/12 hours,
and 10,000/seven days; with a registered key (`X-ClientId`, secret `SOURCE_GUS_BDL_API_KEY`)
10/s, 500/15 min, 5,000/12 h and 50,000/7 days. Response headers `X-Rate-Limit-Limit`,
`X-Rate-Limit-Remaining`, and `X-Rate-Limit-Reset` may require a slower rate. A
complete run is necessarily multi-cycle: the known minimum already exceeds 534,000
requests before continuation pages. Checkpoint each validated page. Conditional
requests and resource `lastUpdate` help, but neither proves global completeness or
identifies every changed variable.

BDL data are published under CC BY 4.0. Attribute GUS/Statistics Poland and identify
BDL as the source. The official documentation states no geography restriction.

Primary evidence:

- [Official English BDL API guide](https://api.stat.gov.pl/Home/BdlApi?lang=en)
- [BDL v1 OpenAPI](https://bdl.stat.gov.pl/api/v1/swagger/doc/swagger.json)

## GUS Dziedzinowe Bazy Wiedzy (DBW)

**Extraction (24 Oct 2026).** The DBW website publishes every indicator as bulk ZIP files
(one per cross-section × presentation × periodicity, some also per year), each with one
semicolon CSV of ids (`id_zmienna;id_przekroj;id_wymiar_1;id_pozycja_1;…;id_okres;
id_sposob_prezentacji_miara;id_daty;id_brak_wartosci;id_tajnosci;id_flaga;wartosc;precyzja`).
The indicator listing (`getAggregatesById`) gives each file with its last-update date, so
entity = file stem (e.g. `1000_156_93_4_pl`), version = that date, and a daily run lands
only files whose date is newer than Landing (`ingestion/to_landing/gus_dbw/extract.py`).
Each ZIP also holds `<id>_Slowniki.csv`, the labels of every id it uses, loaded as `bronze.dbw_dictionary_items` (parsed line by line: GUS leaves quotes inside the descriptions undoubled). No account, no key, no VPN. Bronze keeps the CSV as text; Silver
(`warehouse/entity/dbw_observations.sql`) types the ids and the value (decimal comma).
Labels of the ids come from the DBW dictionaries, loaded later into Gold. The old
pipeline's `01_landing/gus_dbw/native/` and `_control/` stay as they are (owner, 21 Oct).

## Narodowy Bank Polski public statistics

The [NBP Web API](https://api.nbp.pl/en.html) is not a catalogue of all NBP
statistics. Its documented scope is exchange-rate tables A, B and C plus NBP gold
prices. A complete collection for the selected **NBP public-statistics product** must
combine those feeds with the official statistical pages and their downloadable
distributions. This scope does not mean every document published by the NBP
organisation.

### Machine feeds and FX inventory

```text
# Exchange tables; split history into documented date windows
https://api.nbp.pl/api/exchangerates/tables/A/{START}/{END}/?format=json
https://api.nbp.pl/api/exchangerates/tables/B/{START}/{END}/?format=json
https://api.nbp.pl/api/exchangerates/tables/C/{START}/{END}/?format=json

# Gold
https://api.nbp.pl/api/cenyzlota/{START}/{END}/?format=json

# Official historical XML-file inventory and files
https://static.nbp.pl/dane/kursy/xml/dir.txt
https://static.nbp.pl/dane/kursy/xml/{FILENAME_FROM_DIR}.xml
```

The API supports intervals no longer than 93 days and can return `400 Limit
exceeded`; page the dates and treat valid no-data `404` responses as empty windows,
not deletions. The XML `dir.txt` is an official exact filename inventory for the FX
archive and complements the API, especially for representation-level completeness.

**Implemented (FX tables and gold):** `ingestion/to_landing/nbp/` fetches one entity
per table and calendar quarter (`A_2024Q1` … `GOLD_2026Q4`; 356 requests for the full
history, one at a time with a pause). Version = the latest effective date in the
response. Quarters that ended more than 10 days ago are fetched once; open quarters
are re-fetched and written only when the bytes changed. Bronze: `bronze.nbp_observations`
(one row per quote or gold price, values as source text). The yearly CSV/XLS archives
add only pre-2002 history and the broader statistics below are a separate scope (not
yet built).

### Official NBP interest rates (`nbp_interest_rates`)

**Implemented (10 Nov 2026, register PLG-006).** NBP publishes its official rates as two small XML
files on fixed URLs, landed whole through the shared `bulk_file` pattern (version = Last-Modified,
written only when the SHA-256 changed; restart unit: one file):
`https://static.nbp.pl/dane/stopy/stopy_procentowe_archiwum.xml` (entity `rates_archive`: every MPC
change since 1998-02-26, `<pozycje obowiazuje_od>` groups of `<pozycja id oprocentowanie>`; ids `ref`
reference, `lom` lombard, `dep` deposit, `red` rediscount, `dys` discount, `dep` from 2001-12-01, `dys` from 2010-01-01) and
`https://static.nbp.pl/dane/stopy/stopy_procentowe.xml` (entity `rates_current`: the rates in force
with PL/EN names, footnotes and required-reserve rates). The old `www.nbp.pl/xml/…` URL now redirects
to an HTML page. Bronze: `bronze.nbp_interest_rate_items` (one row per `<pozycja>`, attributes as
text, an unknown attribute rejects the file). Silver: `silver.nbp_policy_rates` (rate × effective
date, `valid_to` = next change). Licence: NBP website Terms of Use (source with a link to
www.nbp.pl); public display waits for NBP's answer like the exchange rates. MFI interest-rate
statistics are a separate, not yet built scope.

### Broader statistics inventory and distributions

Crawl only the following official navigation subtrees, follow their pagination/year
pages, and inventory every linked statistical XLS, XLSX, CSV, XML, ZIP, 7Z, TXT and
methodology PDF on `nbp.pl` or `static.nbp.pl`:

```text
https://nbp.pl/en/statistic-and-financial-reporting/
https://nbp.pl/en/statistic-and-financial-reporting/monetary-and-financial-statistics/
https://nbp.pl/en/statistic-and-financial-reporting/balance-of-payments-statistics/
https://nbp.pl/en/statistic-and-financial-reporting/financial-markets/
https://nbp.pl/en/statistic-and-financial-reporting/instruments/
https://nbp.pl/en/statistic-and-financial-reporting/core-inflation/
https://nbp.pl/en/statistic-and-financial-reporting/inflation-expectations-and-forecasts/
https://nbp.pl/en/statistic-and-financial-reporting/rates/
https://nbp.pl/en/payment-system/statistical-data/
```

The monetary subtree includes, among other distributions, M3 and counterparts,
reserve money, MFI assets and liabilities, interest-rate statistics, financial
accounts, banking-sector financial data and Divisia monetary indexes. The
balance-of-payments subtree includes balance of payments, official reserves,
external debt, international investment position and international trade in
services. Financial markets, central-bank instruments, core inflation, inflation
expectations/forecasts, rates and payment-system statistics are separate inventories
and must not be represented by the four Web API feeds.

Known official durable workbook examples are:

```text
https://static.nbp.pl/dane/bilans-platniczy/bop_m.xlsx
https://static.nbp.pl/dane/bilans-platniczy/bop_q.xlsx
https://static.nbp.pl/dane/bilans-platniczy/bop_q_pln.xlsx
https://static.nbp.pl/dane/aktywa-rezerwowe/rez_m.xlsx
```

The first is monthly balance-of-payments history from January 2004, the next two are
quarterly distributions, and `rez_m.xlsx` is monthly official-reserve history from
January 1998. Discover these links from their current source pages rather than using
the examples as a closed allowlist.

Use the [NBP release calendar](https://nbp.pl/en/statistic-and-financial-reporting/calendar/)
as a second freshness signal, but do not treat it as a machine-readable attachment
catalogue. For each attachment retain the source page, displayed label/update date,
URL, response validators when supplied, byte size, retrieval timestamp and SHA-256.
Re-crawl the navigation pages and compare both URL and content hash. No reviewed NBP
document promises byte ranges, so the portable recovery action is to retry one file.

NBP pages and API documentation say the service is public, but the footer says “All
rights reserved,” and the API documentation grants no commercial redistribution or
database licence. The linked Terms of Use were not established here as a grant for
redistribution. Catalogue/download for private analysis can proceed; public or
commercial redistribution remains unresolved until the applicable terms or written
permission are reviewed. No geography restriction was found.

Primary evidence:

- [NBP Statistics landing page](https://nbp.pl/en/statistic-and-financial-reporting/)
- [NBP release calendar](https://nbp.pl/en/statistic-and-financial-reporting/calendar/)
- [NBP Web API documentation](https://api.nbp.pl/en.html)
- [NBP instructions for the historic exchange-rate file inventory](https://nbp.pl/en/statistic-and-financial-reporting/rates/the-instruction-how-to-retrieve-currency-exchange-rates-from-the-nbp-website/)
- [NBP monetary and financial statistics](https://nbp.pl/en/statistic-and-financial-reporting/monetary-and-financial-statistics/)
- [NBP balance-of-payments statistics](https://nbp.pl/en/statistic-and-financial-reporting/balance-of-payments-statistics/)
- [NBP payment-system statistical data](https://nbp.pl/en/payment-system/statistical-data/)
- [NBP regulations / Terms of Use entry point](https://nbp.pl/en/about-nbp/contact/nbps-regulations/)

## GLEIF golden copy

**Extraction (24 Oct 2026).** GLEIF publishes the complete LEI dataset three times a day
(00:00, 08:00, 16:00 UTC) as golden-copy files; the files are ready about 1.5 h after the
publish time. The stable URLs
`https://goldencopy.gleif.org/api/v2/golden-copies/publishes/{lei2|rr|repex}/latest.csv`
redirect (302) to the newest publish's ZIP and give its Last-Modified, so the shared
`bulk_file` pattern lands each file only when its SHA-256 changed (version = Last-Modified).
Sizes on 23 Oct 2026: lei2 3.47 M records, 506 MB ZIP / 5.0 GB CSV, 338 columns; rr 0.49 M
records, 25 MB / 245 MB, 54 columns; repex 62 MB / 716 MB, 12 columns. Full files rather than
the delta files: one complete state a day, each file one entity replaced whole (design #875). Bronze
keeps every CSV column as text; Silver (`warehouse/entity/gleif_*.sql`) types the core LEI, relationship and exception fields. A rejected file leaves the previous version in place.

## MF VAT white list (flat file)

**Extraction (24 Oct 2026).** The Ministry of Finance publishes the "plik płaski" every day at
`https://plikplaski.mf.gov.pl/pliki/<YYYYMMDD>.7z`, valid for that Warsaw date and available
from about 22:30 Warsaw time the evening before. The 7z (~250 MB) holds `<date>.json` (~500 MB)
and its SHA-512. The JSON has a header (generation date, number of hash transformations, scheme)
and three lists: SHA-512 hashes of active taxpayers (3.33 M on 24 Oct 2026), of exempt taxpayers
(0.35 M) and virtual-account masks (1,506). The file carries no NIP or account in clear text:
a NIP + account pair is checked by hashing it the published number of times. The shared
`bulk_file` pattern lands the day's file (`url_date: Europe/Warsaw`, format `7z`, tested with
the runner's 7-Zip); Bronze keeps the list items as published, Silver splits hashes and masks.

## MF State Treasury debt (time series)

**Extraction (2 Nov 2026).** The Ministry of Finance publishes the State Treasury debt time
series as an XLSX workbook linked from the stable page `https://www.gov.pl/web/finanse/szeregiczasowe`;
each release is a new `https://www.gov.pl/attachment/<uuid>`. The declaration
`mf_treasury_debt.yaml` lands it through the shared `bulk_file` pattern as entity
`treasury_debt_series`, finding the link on the page at run time (`url_from`, pattern
`/attachment/`; `landing: inspect` lists every match). Version = Last-Modified (download time
when absent); written only when the SHA-256 changed. Restart unit: the whole workbook. Terms:
gov.pl (register PLG-008). **Bronze to Gold (11 Nov 2026).** The workbook's cells go to `bronze.mf_treasury_debt_cells` (one row per non-empty cell: sheet, row, column, source text, number format; shared decoder `ingestion/common/xlsx_cells.py`, latest-only). Silver `silver.treasury_debt`: every time-series sheet in long form (macro `xlsx_wide_series`: a block starts at a row of dates, items are the column-1 labels below it; repeated labels such as currencies are told apart by `item_occurrence`), month ends from 2001. Gold: build stage `poc_build_public_finance.sql` (debt by instrument, million PLN, monthly). Schedule: weekdays 13:45 UTC. Dev probes 43084312730, 43085156243 reached www.gov.pl from Actions (an earlier refusal seen by the release-calendar probe did not recur); api.dane.gov.pl timed out from Actions, so it is not an alternative route.

## MF public finance sector debt

**Extraction (2 Nov 2026).** The Ministry of Finance publishes the quarterly public finance
sector debt workbook (`Dlug_publiczny_<quarter>.xlsx`) as the second attachment of the stable page
`https://www.gov.pl/web/finanse/szeregiczasowe` (the first is the Treasury debt workbook); the page
`zadluzenie-sektora-finansow-publicznych` has only PDF reports. Each release is a new
`https://www.gov.pl/attachment/<uuid>`. The declaration `mf_public_debt.yaml` lands it through the
shared `bulk_file` pattern as entity `public_debt` (`url_from`, pattern `/attachment/`, `pick: last`;
`landing: inspect` lists every match). Version = Last-Modified (download time when absent); written
only when the SHA-256 changed. Restart unit: the workbook. Terms: gov.pl (register PLG-009). **Bronze to Gold (11 Nov 2026).** The workbook's cells go to `bronze.mf_public_debt_cells` (one row per non-empty cell: sheet, row, column, source text, number format; shared decoder `ingestion/common/xlsx_cells.py`, latest-only). Silver `silver.public_debt`: the six sheets in long form (`xlsx_wide_series`), unit `mln_pln` or `ratio` (the '%'-formatted debt-to-GDP rows). Gold: build stage `poc_build_public_finance.sql` (debt to GDP for State Treasury, PDP and general government EDP debt, quarterly and the 31 December value as the year (measure suffix `:YE`), which is the MF series of the v2 KPI `gg_debt_gdp` next to Eurostat (O31); the same debts and EDP debt by ESA instrument in million PLN). Schedule: Wednesdays 13:55 UTC. Dev probes 43084312730, 43085156243 reached www.gov.pl from Actions (an earlier refusal seen by the release-calendar probe did not recur); api.dane.gov.pl timed out from Actions, so it is not an alternative route.

## MF state budget execution (monthly)

**Extraction (2 Nov 2026; one entity per year since 11 Nov).** The Ministry of Finance publishes the
monthly operational report on state budget execution on per-year pages
`https://www.gov.pl/web/finanse/sprawozdania-miesieczne-<year>`, as xlsx/pdf pairs, newest first (pages
2018–2026 list XLSX; 2010–2017 list none); each release is a new `https://www.gov.pl/attachment/<uuid>`.
Table 1 of every report repeats each month of its year so far (cumulative from January), so the newest
report of each year holds the whole year. `mf_state_budget.yaml` declares the file once with
`years: {from: 2018}`: the `bulk_file` pattern expands it to entities `budget_execution_2018` …
`budget_execution_<current year>` (`url_from` page with `{year}`, `pick: first`); the current year's entity is
optional, so January, before the first report, is not a failure, and the yearly rollover needs no code
change. With `years.open: 2` only the current and the previous year are downloaded again; older years
land once (their December report is final). Version = Last-Modified; written only when the SHA-256 changed. Terms: gov.pl (register PLG-007).
**Bronze to Gold (11 Nov 2026).** Cells in `bronze.mf_state_budget_cells` (latest-only per year entity);
Silver `silver.budget_execution`: table 1, one row per year × month × item × measure (`execution_ytd`
cumulative, `plan` = budget act), thousand PLN, `item_key` for revenue, expenditure, balance, EU funds
result and financing (domestic, foreign). Gold (`poc_build_public_finance.sql`): execution monthly and the
December value as the year (measure suffix `:A`, so it never shares a series × date with January), plan yearly. Tables 2–20 are in Bronze, not yet modelled. Schedule: weekdays
13:35 UTC.

## OpenData.org (BrightQuery)

**Extraction (owner decision 25 Oct 2026, #906: keep and run through the whole process).**
BrightQuery publishes OpenData.org snapshots in the public Google Cloud Storage bucket
`bq-public-datastore`: `opendata/ODO-SENZING-<date>/full/ODO_SENZING_<date>.zip` (newest
5 Mar 2026, 21.4 GB) plus per-type ZIPs, listed by the GCS JSON API with size, MD5 and update
time (no account). The ZIP holds 2,496 newline-delimited Senzing JSON files: Organization/
(501 files, 44 GB), PeopleBusiness/ (494, 46 GB) and Locations/ (1,501, 68 GB); each record is
`{DATA_SOURCE, RECORD_ID, bq_dataset, FEATURES: [ {...}, ... ]}`. Coverage: United States
(86 M organizations, 101 M business contacts, 142 M locations, per the publisher). The publisher
states no licence; the data includes names of people (business contacts, LinkedIn URLs).
Entity = one member file; Bronze keeps the records as published, Silver
(`warehouse/entity/opendata_*.sql`) takes the main Senzing features per record type. A monthly
run (owner, 25 Oct 2026) checks the bucket; a new snapshot (~21 GB) replaces all files.

## GUS TERYT (territorial register)

Catalogues: TERC (territorial units: voivodeship, powiat, gmina with RODZ), SIMC
(localities, `SYM`/`SYMPOD`), ULIC (streets per locality, `SYM_UL`), WMRODZ (locality
types). Each comes in variants: *urzędowy* (official), *adresowy* (for addressing) and,
for SIMC, *statystyczny* (with statistical subdivisions `NMST`, `NMSK`, `SYMSTAT`).

**Route (implemented):** the public full-file page
`https://eteryt.stat.gov.pl/eTeryt/rejestr_teryt/udostepnianie_danych/baza_teryt/uzytkownicy_indywidualni/pobieranie/pliki_pelne.aspx`
serves each catalogue variant as a ZIP holding the same data as XML and as CSV
(`;`-separated, UTF-8 with BOM). The page is an ASP.NET form: one GET for the form
state, then one POST per file with `__EVENTTARGET` = the file's download control
(`ctl00$body$BTERCUrzedowyPobierz` …). No account, no browser automation. The SOAP
service TERYT ws1 offers the same full files plus change files but needs an account
(e-mail to teryt_ws1@stat.gov.pl); not used.

Version = the catalogue state date in the XML (`<catalog … date="YYYY-MM-DD">`): TERC
and SIMC state 1 January unless legal changes intervene; ULIC is restated daily. Sizes
(October 2026, ZIP): TERC 0.13 MB, SIMC 3.2–4.5 MB, ULIC 8.1 MB, WMRODZ 0.05 MB.

Bronze `bronze.teryt_records`: the CSV member, every column as text, mapped onto the
union of catalogue columns. Change files (zmiany) are not loaded; successive full
states give the history.

## GUGiK PRG (national register of boundaries)

**Route (implemented):** the national package
`https://opendata.geoportal.gov.pl/prg/granice/00_jednostki_administracyjne.zip` (~377 MB,
shapefiles, geographic ETRS89 coordinates per the `.prj`): A00 country (1), A01 voivodeships (16), A02 powiats (380), A03
gminas (2,479), A05 cadastral units (3,227), A06 cadastral precincts (54,264), counts of
October 2026. Every layer carries the JPT attribute set (`JPT_KOD_JE` = TERYT code,
`JPT_NAZWA_`, `JPT_POWIER` area, validity `WAZNY_OD/DO`, version `WERSJA_OD/DO`, `REGON` for
A01–A03). The server sends no Last-Modified or ETag: every run downloads (~7 minutes from
GitHub Actions) and writes Landing only when the SHA-256 changed. GUGiK states the data is
free for any purpose (Regulation of the Council of Ministers of 16 July 2021, Dz.U. 2021
poz. 1373); updated yearly (state 1 January) with corrections. WFS
(`https://mapy.geoportal.gov.pl/wss/service/PZGIK/PRG/WFS/AdministrativeBoundaries`, GML,
1,000 features per request) is the alternative, not used.

Bronze `bronze.prg_boundaries`: one row per feature of every layer, attributes as text,
geometry as WKB in the source CRS with the `.prj` text (60,367 features, ~434 MB Parquet); any reprojection is a Silver rule.

## UN World Population Prospects (WPP)

**Extraction (2 Nov 2026).** The UN Population Division publishes each WPP revision as fixed
files; the declaration `un_wpp.yaml` lands the 2024 revision's "Demographic Indicators, medium
variant" CSV (gzip) through the shared `bulk_file` pattern as entity
`demographic_indicators_medium` (version = Last-Modified, download time when absent; written
only when the SHA-256 changed, after the gzip check). Restart unit: the whole file (one
download). Licence CC BY 3.0 IGO (register GL-039). The URL is from the register review and is
proven from Actions by the Dev trial; a new revision (every two years) means a new URL in the
declaration.

**Implemented (11 Nov 2026).** A second file, `population_by_age5_medium` (population by five-year age
group and sex, 1,759,905 rows, 30 MB gzip), gives the dependency ratios. Bronze: `bronze.un_wpp_rows`,
generic CSV rows (one header row per file with `field_names`, data rows with `field_values`, text).
Silver: `silver.un_wpp_demographic_indicators`
(555 locations × 1950-2101, 54 indicators typed) and `silver.un_wpp_population_age_groups` (broad age
groups summed from the five-year groups, dependency ratios; the five-year rows stay in Bronze, above the
~1M full-rebuild policy). Gold: `gold.build_un_wpp` for the EU27 members, the WPP group "European Union
(EU: 27)" (LocID 1551, EU27_2020) and World; every indicator is an `:estimate` (to 2023) and a `:projection`
(2024-2100, status forecast) measure; values, unit and scale as published (D33; population in thousands); latest revision only, no vintages; areas through
`poc_map_geography` (section `un_wpp`, static codes); draft KPIs total_fertility_rate, life_expectancy,
old_age_dependency (computed from the five-year age groups) and population_projection. Licence `unresolved`: no UN
page states the WPP terms (Dev probe 43098349843), so WPP is kept out of the published layer: no row for the
published KPI population, and its `kpi_v2_map` rows are needs-block. Monthly check (3rd, 06:25 UTC), full rebuild (D32).

## World Bank Global Findex Database

**Extraction (2 Nov 2026).** The country-level Global Findex database is one CSV per edition
(2025: `GlobalFindexDatabase2025.csv` on thedocs.worldbank.org), linked from the stable page
`https://www.worldbank.org/en/publication/globalfindex/download-data`. The declaration
`world_bank_findex.yaml` finds the link on that page (`url_from`, pattern
`GlobalFindexDatabase<year>.csv`) and lands it through the shared `bulk_file` pattern as entity
`findex_country` (version = Last-Modified, download time when absent; written only when the
SHA-256 changed), so a new edition is picked up without a code change. Restart unit: the whole
file. Licence CC BY 4.0 for the country-level indicators only (register GL-009); the
individual-level microdata (Research Data License) is out of scope.

**Bronze and Silver (11 Nov 2026, #2736).** Bronze `bronze.findex_observations`
(`ingestion/to_bronze/world_bank_findex/`): the wide file kept long, one row per economy × survey year ×
population group (`group`/`group2`) × indicator column with a value (`NA` not stored), values as text; key
columns found by name prefix, since `regionwb24_hi`/`incomegroupwb24` carry the edition. Silver
`silver.findex_observations` (typed, shares 0–1). 2025 edition: 385,599 observations, 174 economies and
aggregates (World Bank regions and income groups, no EU aggregate), 430 indicators, waves 2011–2024
(Dev trial 43085820597). Weekly check Thursday 02:20 UTC.
## World Bank Worldwide Governance Indicators (WGI)

**Extraction (2 Nov 2026).** DataBank publishes WGI as a bulk CSV ZIP on a stable URL, the same
route as WDI (`https://databank.worldbank.org/data/download/WGI_CSV.zip`). The declaration
`world_bank_wgi.yaml` lands it through the shared `bulk_file` pattern as entity `WGI_CSV`
(version = Last-Modified, download time when absent; written only when the SHA-256 changed).
Restart unit: the whole file (one download). Licence CC BY 4.0 (register GL-008), attribution
"Source: World Bank, Worldwide Governance Indicators". WGI is revised once a year.

**Bronze and Silver (11 Nov 2026, #2736).** Bronze `bronze.wgi_observations` and `bronze.wgi_metadata`
through the shared DataBank decoders (`ingestion/common/databank_bronze.py`, members identified by their
headers; observations long, empty cells not stored; metadata as field/value rows). Silver
`silver.wgi_observations` and `silver.wgi_indicators`. The September 2026 archive: 202,092 observations,
216 countries and territories (no aggregates, so no EU value), 36 indicators = six dimensions × estimate
(EST), score 0–100 (SC) with 90 % bounds, standard error and number of sources; 1996, 1998, 2000, then
yearly to 2025 (Dev trial 43085820597). Weekly check Thursday 01:20 UTC.

## UNCTAD Data Hub (UNCTADstat)

**Extraction (2 Nov 2026).** Every Data Hub table has a bulk file: one 7z archive with the whole
table as CSV, at a stable URL per report code
(`https://unctadstat-api.unctad.org/bulkdownload/US.PLSCI/US_PLSCI`). The declaration `unctad.yaml`
lands nine tables through the shared `bulk_file` pattern, one entity per table: FDI flows and
stocks, merchandise trade, services trade by category, digitally deliverable services, monthly
commodity prices, liner shipping connectivity, merchant fleet, seaborne trade and port calls (about
14 MB compressed in all). Tables that other sources already cover (GDP, population, CPI, exchange
rates) and the large creative-economy and bilateral-services tables are not declared. The server
sends no Last-Modified, so the version is the download time and a file is written only when its
SHA-256 changed. Restart unit: one table. Licence CC BY 3.0 IGO (register GL-021), attribution
"Source: United Nations Trade and Development (UNCTAD) Data Hub, unctadstat.unctad.org"; tables
with third-party series keep their own terms.

**Implemented (11 Nov 2026).** Every table's CSV has one shape: a time field (Year, Period or Quarter),
code/label pairs (Economy, Flow, Category…) and measures, each with "Footnote" and "Missing value"
columns. Bronze: `bronze.unctad_rows`, generic CSV rows (`field_names` / `field_values`, `|`-separated:
column names contain commas), extracted with the runner's 7-Zip. Silver: one model per table
(`silver.unctad_fdi_flows_stock`, `unctad_merchandise_trade`, `unctad_services_trade`,
`unctad_digital_services_trade`, `unctad_commodity_prices`, `unctad_liner_shipping_connectivity`,
`unctad_merchant_fleet`, `unctad_seaborne_trade`, `unctad_port_calls`) from the macro `unctad_table`:
period, economy, the other dimensions and the measures (value, footnote, missing reason) as JSON maps by
name. Gold: `gold.build_unctad` (FDI flows and stocks, merchandise and services exports and imports) for
the EU27 members (UNCTAD codes, France 250), the group "European Union (2020 …)" (3412, EU27_2020) and
World; KPIs fdi_inflow, exports_goods and exports_services, draft until the per-table third-party terms
(register GL-021 blocker; merchandise trade is synchronised with WTO) are checked. Weekly schedule (Tuesday
05:55 UTC), full rebuild when a table's bytes change (D32: each archive is the whole table); the services
table (807,733 rows) is rebuilt in full under the exception #2777 to #822.

## ECB Data Portal

**Extraction (2 Nov 2026).** The ECB Data Portal has no bulk archive per dataflow, but its SDMX
REST API (`https://data-api.ecb.europa.eu/service/data/<flow>/<key>?format=csvdata`) returns a
whole selection, every series with its full history, in one response. The declaration `ecb.yaml`
lands four fixed selections through the shared `bulk_file` pattern, one request per entity and
run: `exr_daily` (EXR, euro reference rates of every currency), `fm_key_rates` (FM, key ECB
interest rates), `est_estr` (EST, €STR) and `icp_annual_rate` (ICP, HICP annual rate, every area).
Version = Last-Modified, download time when absent; written only when the SHA-256 changed.
Restart unit: one entity (one request). Entity: the selection, one file of SDMX CSV
observations with their attributes. Re-use under the ECB statistics usage policy (register
GL-016): free, also commercial, attribution "Source: ECB statistics", statistics not modified.
The queries are proven from Actions by the Dev trial (#2393).

**Implemented (11 Nov 2026).** Three more selections: `hicp_annual_rate` (HICP, the COICOP 2018 dataflow
that replaced ICP on 4 Feb 2026; ICP stops at 2025-12 and stays as the old method's history),
`irs_long_term` (IRS, long-term interest rates for convergence purposes, every EU member incl. Poland
from 2001-01) and `bsi_monetary_aggregates` (BSI, euro area M1–M3 stocks, index and annual growth,
seasonally adjusted, from 1980). Bronze: `bronze.ecb_observations`, the generic SDMX format of
`bronze.oecd_observations` (KEY split into `dataflow` and `series_key`). Silver: `silver.ecb_observations`
(all selections, typed, dimensions JSON, period_start). Gold: `gold.build_ecb` (blocks in
`poc_ecb_blocks`: 13 EXR currencies, key rates, €STR, HICP/ICP, IRS, BSI); KPIs fx_rate (EUR/PLN),
inflation (HICP), bond_yield_10y (IRS), m3_growth and ecb_deposit_rate. Areas: ISO two-letter countries
(GR), U2 → EA, I8 → EA19, I9 → EA20, B6 → EU27_2020 (fixed composition), B0/D0 → EU (changing composition). Weekly schedule
(Thursday 05:40 UTC), full rebuild (D32: about 300,000 rows).

## World Bank International Comparison Program (ICP)

**Extraction (2 Nov 2026).** ICP has no bulk feature: DataBank publishes no bulk file for it
(`ICP_CSV.zip` and `ICP_2021_CSV.zip` return 404) and the ICP pages link only DataBank views. The
route is the World Bank API v2, source 90 (ICP 2021), tested for a full load by a Dev probe:
48 series, 1,182,384 observations, about 380 MB of JSON at 50,000 records per page. The
declaration `world_bank_icp.yaml` uses its own extractor (`ingestion/to_landing/world_bank_icp/`):
entity = one series for every economy and year
(`/v2/sources/90/country/all/series/<id>/time/all?format=json&per_page=50000`, one file per
page when a series is larger), version = the source's `lastupdated` date, so a series is
downloaded again only when the World Bank updates ICP. Restart unit: one series. Licence CC BY
4.0 (register GL-011). A series page can take minutes to generate when the API has no cached copy (prod run
43157799551): 7-minute timeout, six retries with backoff; a series that still fails is recorded and the
others still land, the run ends red and the next run fetches the missing series.

**Bronze and Silver (11 Nov 2026, #2736).** Bronze `bronze.icp_observations`
(`ingestion/to_bronze/world_bank_icp/`, shared loader `bulk_bronze.py` over the series folders in
Landing): one row per record with a value, the codes and labels of Country, Classification, Series and
Time, the value as the exact JSON number text. Silver `silver.icp_observations`. 231,174 observations,
207 economies and benchmark groups (ECB, HIB, LIB, LMB, UMB, WLD; no EU aggregate), 48 series × 17
classifications (expenditure in LCU and US$, PPP, per capita PPP, price level index world = 100, …),
2017–2023 (Dev trial 43085820597). Archive retention: newest version + 90 days. Weekly check Thursday 02:50 UTC (two catalogue requests while unchanged).

## KRUS basic benefit statistics

**Extraction (2 Nov 2026).** KRUS publishes its basic beneficiary statistics on a stable gov.pl
page as one XLSX (about 20 kB) whose attachment URL changes with each update. The file is a
rolling series, not one quarter: the Dev trial (#2468) found one sheet `Świadczenia` (A1:U62) with
quarterly columns from I quarter 2020 to the newest quarter and yearly totals, for the average
monthly number of beneficiaries and the average monthly benefit in PLN: pensions and disability
pensions in all, old-age pensions, disability pensions (of which accident), family pensions,
parental supplementary benefits and benefits for civilian blind war victims. The declaration
`krus.yaml` finds the link on the page (`url_from`, the page's only `/attachment/` link) and lands
it through the shared `bulk_file` pattern as entity `swiadczenia_kwartalne`. The server sent no
Last-Modified in the trial, so the version is the download time and a file is written only when
its SHA-256 changed; each version is the whole series to date (`snapshot`). Older versions can hold quarters that a later file drops if KRUS shortens the window,
so `prune` stays `report`. Restart unit: the file. Not declared: the quarterly statistical
information PDFs (51 tables), which have no stable listing page. Terms: gov.pl, official materials
free to use (register PLS-011). **Bronze to Gold (11 Nov 2026).** Cells in `bronze.krus_cells`
(latest-only); Silver `silver.krus_benefits`: one row per period (quarter or year) × benefit type × measure
(`beneficiaries`, `avg_benefit_pln`), from I quarter 2020; benefit types from the header rows (group, then
"Ogółem" / "w tym wypadkowe"); an unknown column fails the `benefit_key` not-null test. The footnote mark
of III quarter 2025 on (survivors' pensions including those paid with other pensions) is kept in
`footnote` and becomes observation status `break` in Gold: a series break. Gold (`poc_build_public_finance.sql`): every benefit type and measure, quarterly
and yearly. Schedule: Wednesdays 14:05 UTC.

## World Bank International Debt Statistics (IDS)

**Extraction (3 Nov 2026).** DataBank publishes IDS as a bulk CSV ZIP on a stable URL, the same
route as WDI and WGI (`https://databank.worldbank.org/data/download/IDS_CSV.zip`). The declaration
`world_bank_ids.yaml` lands it through the shared `bulk_file` pattern as entity `IDS_CSV`
(version = Last-Modified, download time when absent; written only when the SHA-256 changed).
The Dev trial (#2378) landed 11.1 MB (Last-Modified 5 Dec 2025): `IDS_ALLCountries_Data.csv`
(62,983 rows, 137 debtor countries and groups, year columns 1970–2032 with
projections, 35 % of year cells empty) and four metadata CSVs (country, 574 series, country-series
and footnotes). Restart unit: the whole file (one download). Licence CC BY 4.0 (register GL-007), attribution
"Source: World Bank, International Debt Statistics". IDS is published once a year in December.

**Bronze and Silver (11 Nov 2026, #2736).** Bronze `bronze.ids_observations` and `bronze.ids_metadata`
through the shared DataBank decoders (`ingestion/common/databank_bronze.py`; IDS adds the counterpart
area and pads some codes with spaces, which Silver trims). Silver `silver.ids_observations` and
`silver.ids_series` (the series member plus one series the observations use without a metadata row, DT.TDS.DECT.GN.ZS, `in_series_metadata` false). The November 2026 archive: 2,647,628 observations, 134 debtor economies and groups,
577 series, counterpart WLD only, 1970–2033 with projections (Dev trial 43085820597). IDS has no Poland
and no EU member state (debtors are low- and middle-income economies), so it publishes no KPI for the
zohelo slice. `silver.ids_observations` is rebuilt in full above the #822 1M-row ceiling under its own exception record
(#2772: 4M ceiling; every archive revises the whole history; rebuilt on a new archive and on any change to the
shared build fingerprint; review 15 Jan 2027).
Weekly check Thursday 01:40 UTC.

## OECD Data Explorer (SDMX)

**Implemented (10 Nov 2026, register GL-003).** The OECD Data Explorer has no bulk file per dataflow;
its SDMX REST API returns a whole selection with full history in one response
(`https://sdmx.oecd.org/public/rest/data/<agency>,<dataflow>,<version>/<key>?format=csvfile`;
structure: `…/dataflow/<agency>/<dataflow>/latest?references=datastructure`). The declaration
`oecd.yaml` lands each selection through the shared `bulk_file` pattern, one request per entity and
run (the API is rate limited without a published threshold). The server's Last-Modified is the
structure date, so version = download time; a copy is written only when the SHA-256 changed. Entity
`cli`: dataflow `OECD.SDD.STES:DSD_STES@DF_CLI` (version 4.1), all areas: 240,988 observations,
57 areas (members, partners, OECD, G7, G20, EA20, EU), 21 MB (Dev probe 42776278659, Dev trial
42777288678). Poland, EU, EA20 and OECD have business and consumer confidence (BCICP, CCICP); the
amplitude-adjusted CLI (LI) exists for 22 areas only (G20 economies, G7, G20, A5M, G4E, NAFTA).
Bronze: `bronze.oecd_observations` (generic SDMX CSV rows: series_dimensions/series_key,
attribute_names/attribute_values, text). Silver: `silver.oecd_cli`. Terms: OECD Terms and
Conditions (data reuse incl. commercial, with attribution; dataset-specific third-party limits are
checked per added dataflow). Restart unit: one selection.

## UNDP Human Development Data Center (HDI)

**Built (11 Nov 2026, register GL-028).** The downloads page
(`https://hdr.undp.org/data-center/documentation-and-downloads`) links one CSV with every composite index
and component for every country and aggregate (HDR25: `…/2025_HDR/HDR25_Composite_indices_complete_time_series.csv`,
2.0 MB). Its URL changes with each report, so `undp_hdr.yaml` finds the link on the page (`url_from`);
a copy is written only when the SHA-256 changed. Bronze: `bronze.undp_hdr_values`, one row per non-empty
cell (id columns, source column name, value as text). Silver: `silver.undp_hdr_indices` (iso3 × indicator ×
year; indicators hdi, le, eys, mys, gnipc, gdi, gii, ihdi, phdi, components and sex variants; aggregates
`ZZ…`): 200,004 values, 206 areas, 1990–2023 (Dev trial 43093051337; Poland HDI 0.906, rank 35 in 2023).
Each report recalculates the whole series, so values compare within one release; the Archive keeps the
previous report 400 days. Terms: CC BY 3.0 IGO, licence row `cc_by_3_0_igo_undp_hdr`. Monthly check.
Restart unit: the file.
## WHO data.who.int (indicator files)

**Built (12 Nov 2026, register GL-025).** Every data.who.int indicator page links its complete data as one CSV,
`https://srhdpeuwpubsa.blob.core.windows.net/whdh/DATADOT/INDICATOR/<uid>_ALL_LATEST.csv`. `who.yaml` lands two
through the shared `bulk_file` pattern, version = the file's Last-Modified: `life_expectancy` (90E2E48,
WHOSIS_000001) and `hale` (C64284D, WHOSIS_000002), every country, WHO region, income group and World, 2000–2021,
by sex: 25,872 rows (Dev trial 43159213097). This is the CC BY 4.0 route to the GHO indicators; the GHO OData
API (GL-024) stays parked for its terms. Bronze: `bronze.who_observations` (CSV columns lower-cased, values as
text). Silver: `silver.who_observations` (grain entity × geo_type × geo_code_m49 × year × sex, value with
bounds). Gold: `gold.build_who` — both indicators for the EU27 members (M49, macro `poc_m49_eu27`) and World, sex
as breakdown (KPI life_expectancy). Refresh full (D32). Terms: CC BY 4.0 with WHO's additional terms, licence
row `cc_by_4_0_who`. Restart unit: one indicator file.

## UNESCO UIS (Data API selections)

**Built (12 Nov 2026, register GL-026).** The UIS bulk files (BDDS) hold whole themes under a release folder
(SDG.zip 34 MB, OPRI.zip 68 MB in 202602); the public UIS Data API returns an indicator's complete data in one
request (`https://api.uis.unesco.org/api/public/data/indicators?indicator=<code>`, at most 100,000 records).
`unesco_uis.yaml` lands two whole-indicator selections through the shared `bulk_file` pattern, version =
download time (unchanged bytes skipped): `gerd_gdp` (EXPGDP.TOT, GERD as % of GDP, 1996–2024) and
`tertiary_attainment` (EA.5T8.AG25T99 with .F and .M, short-cycle tertiary or higher, population 25+,
1970–2025): 10,689 records, every country and region (Dev trial 43159213097). Bronze:
`bronze.unesco_uis_observations` (one row per record, DuckDB JSON reader). Silver:
`silver.unesco_uis_observations` (grain entity × indicator_id × geo_unit × year). Gold: `gold.build_unesco_uis`
for the EU27 members (ISO alpha-3), sex as breakdown for attainment (KPIs gerd_gdp, tertiary_attainment).
Refresh full (D32). Terms: CC BY-SA 4.0: values derived from UIS data are published under CC BY-SA 4.0 with the credit "Source:
(Adapted from) UNESCO Institute for Statistics (UIS), <URL>, <date of extraction>"; licence row `cc_by_sa_4_0_uis`.
The UIS `kpi_v2_map` rows stay `needs-block` until publishing carries that licence per series (#2816). Restart unit: one selection.

## FAOSTAT (bulk domain files)

**Built (12 Nov 2026, register GL-022).** FAOSTAT publishes each domain as one zip of the whole domain ("All
Data (Normalized)"), listed with size, rows and update date in the bulk catalogue
`https://bulks-faostat.fao.org/production/datasets_E.json`. `faostat.yaml` lands two through the shared
`bulk_file` pattern, one request per entity and run, version = the file's Last-Modified: `emissions_totals`
(GT, agrifood systems emissions totals, 20.6 MB zip, 2,498,428 rows) and `land_use` (RL, 3.0 MB, 421,850
rows), 278–284 areas from 1961 (Dev trial 43159213097). Population (OA) is UN WPP re-disseminated (third-party
data, onboarded as `un_wpp`) and not landed. Bronze: `bronze.faostat_observations` (one row per published row,
values as text, DuckDB CSV reader). Silver: `silver.faostat_observations` (grain entity × area_code ×
item_code × element_code × year × source_code). Gold: `gold.build_faostat` — GHG emissions all sectors without
LULUCF (kt CO2eq, AR5, 1990–2024; KPI ghg_emissions) and land area (1000 ha as published, 1961–2024; KPI area,
whose unit is km² — 1000 ha = 10 km², converted only in a consumer, never in Silver or Gold) for the EU27
members (M49 codes, macro `poc_m49_eu27`), European Union (27) and World; flags E and I → estimated; projected
years (after the file's version year, Silver `is_projection`) are not built. Refresh full (D32), ceiling
exception #2815. Terms: CC BY 4.0 with FAO's additional terms (no use for the promotion of a commercial
enterprise), licence row `cc_by_4_0_fao`. Restart unit: one domain file.

## UNdata (UN Statistical Yearbook tables)

**Built (12 Nov 2026, register GL-017).** UNdata's legacy data-mart download handlers now return the
UN System Data Commons page, so the maintained product is used: the UN Statistical Yearbook CSV tables
linked from `https://data.un.org/Default.aspx` (`_Docs/SYB/CSV/SYB68_<table>_202511_<title>.csv`). The
file names change per edition, so each entity finds its CSV link by table number (`url_from`); a copy is
written only when the SHA-256 changed. 27 tables of one layout (title line, then
`Region/Country/Area,,Year,Series,Value,Footnotes,Source`); five tables with extra columns (SYB 123,
130, 176, 317, 330) are left out until modelled. The files are Windows-1252 with a few undefined bytes,
decoded losslessly (Latin-1 plus the Windows-1252 0x80–0x9F characters). Bronze:
`bronze.undata_syb_values` (rows as published). Silver: `silver.undata_syb_values` (M49 area code,
year, series with its unit in the name, value without thousands separators): 126,703 rows, 1975–2025
(Dev trial 43163356932). Each row cites its source agency. Terms: UNdata conditions of use (free reuse
citing UNdata; commercial use not addressed), licence row `undata_terms` with `commercial_ok` false.
Monthly check. Restart unit: one table.

## BIS Data Portal (SDMX)

**Built (11 Nov 2026, register GL-015).** The BIS Data Portal has csv_flat bulk zips per dataflow
(`https://data.bis.org/bulkdownload`), but they decorate every code with its label, put series
attributes on separate rows and carry the daily series (WS_CBPOL 476 MB unzipped). The SDMX REST API
returns the same dataflow as SDMX-CSV 2.0 codes (`https://stats.bis.org/api/v1/data/<dataflow>/<key>/all?format=sdmx-csv`),
one response per selection. `bis.yaml` lands six selections through the shared `bulk_file` pattern, one
request per entity and run: `cbpol` (WS_CBPOL, monthly), `eer` (WS_EER, monthly), `spp` (WS_SPP), `tc`
(WS_TC), `credit_gap` (WS_CREDIT_GAP), `dsr` (WS_DSR); 371,000 observations, 64 MB, Poland in all six
(Dev probe 43087048719). No Last-Modified, so version = download time; a copy is written only when the
SHA-256 changed. Bronze: `bronze.bis_observations` (the generic SDMX format of the OECD onboarding).
Silver: `silver.bis_observations` (dimensions as JSON, area from REF_AREA or BORROWERS_CTY, series
attributes sent on rows without a period filled onto their observations, `NaN` as no value). Terms:
BIS permitted use of statistics (cite the BIS; no additional charge to users; no warranty), licence row
`bis_terms`. Weekly Thursday check. Restart unit: one selection.

## ILOSTAT (bulk indicator files)

**Built (11 Nov 2026, register GL-023).** ILOSTAT's bulk download page
(`https://rplumber.ilo.org/files/website/bulk/indicator.html`) links one gzip CSV per indicator and
frequency with every country, ILO region and period (`https://rplumber.ilo.org/data/indicator/?id=<id>&format=.csv.gz`).
`ilostat.yaml` lands five through the shared `bulk_file` pattern, one request per entity and run:
`une_deap_sex_age_rt_a` / `_q` (unemployment rate by sex and age), `emp_dwap_sex_age_rt_a`
(employment-to-population ratio), `eap_dwap_sex_age_rt_a` (labour force participation rate),
`eip_neet_sex_rt_a` (youth NEET rate): 1,718,986 observations, all 27 EU countries, Poland from the
LFS (source BA:432) 2000–2025 annual and 2000Q1–2026Q2 quarterly (Dev trial 43091444753). National
(reported) series only; ILO modelled estimates are separate indicators. No Last-Modified, so version =
download time; the bytes are stable between downloads, so an unchanged file is skipped. Bronze:
`bronze.ilostat_observations` (the generic SDMX format of the OECD onboarding: dimension columns
ref_area, source, indicator, sex, classif1 as series_key; obs_status and notes as attributes). Silver:
`silver.ilostat_observations` (ref_area ISO alpha-3 or ILO aggregate X…, typed value, frequency from the
entity). Refresh full (D32): a narrow table rebuilt weekly in full, ceiling exception #2832. Terms:
CC BY 4.0 for releases from 3 May 2023, licence row `cc_by_4_0_ilo`. Restart unit: one indicator file.

## UNECE Statistical Database (PXWeb)

**Built (11 Nov 2026, register GL-038).** No bulk feature: the database (PXWeb 2015) does not serve
its PX files, and its PXWeb API v1 (`https://w3.unece.org/PXWeb2015/api/v1/en/STAT`) answers data only
to POST queries. `ingestion/to_landing/unece/` therefore lists the tables of the economic statistics
folder `20-ME` (macroeconomic overview, national accounts, labour force and wages, price indices,
industrial production, external economic relations, globalization) and queries each whole as JSON-stat
2.0; a table above the API cell limit (HTTP 403) is queried per value of its first variable and lands
as entities `<table>__p<value code>` (Silver keeps only the newer of the whole and part shapes). Version = the table's `updated` time (the API restamps tables daily, so the
SHA-256 decides whether a copy is written). One request at a time with a 4 s pause: the API answers
bursts with 429. Bronze: `bronze.unece_observations` (the generic SDMX format of the OECD onboarding:
variable codes as series_key (`|`-separated), period label, value and status) and `bronze.unece_codes` (variable and
value labels per file). Silver: `silver.unece_observations` (codes and labels as JSON, area = the
Country variable: M49 codes and aggregates such as E27 = EU-27). Terms: UNECE copyright notice (free
reuse incl. commercial, acknowledge UNECE), licence row `unece_terms`. Weekly Monday check. Restart
unit: one table.

## News headline feeds (`news_feeds`, topics)

**Implemented (10 Nov 2026, register TP-001…TP-008, family J; topics layer O32, [topics.md](topics.md)).**
One register entry per outlet; the feeds are complete small files at fixed URLs, landed whole through the
shared `bulk_file` pattern every 3 hours (one request per feed and run; a copy is written only when the
SHA-256 changed). Feeds (Dev probe 42784534992): Bankier.pl `rss/wiadomosci.xml` (RSS, 50 items),
Money.pl `rss/rss.xml` (RSS, 15 items, ~20 h), Business Insider Polska `gospodarka.feed` (RSS, 20 items,
~3 days; the main `/.feed` rolls over in ~3 h), Rzeczpospolita `rss/451-ekonomia` (RSS, 100 items, ~6
days), Puls Biznesu `rss/najnowsze.xml` (RSS, 60 items, local time without offset), Forsal `/.feed`
(Atom, 50 entries), Notes from Poland `/feed/` (WordPress RSS, English). robots.txt allows every feed;
no outlet publishes feed-specific terms, their site terms reserve rights to the content, so only the
headline, link and outlet name are used. PAP (TP-007) is parked: `www.pap.pl/rss.xml` answers automated
clients with a bot-protection page. Bronze `bronze.news_items` is accumulating (every landed copy, not
latest-only; `bulk_bronze.load(all_versions=True)`): title, link, guid, outlet, feed language and
publication time (text and UTC) only, never descriptions, article text or images. Silver
`silver.news_headlines`: deduplicated by canonical URL, then outlet + normalised title on the same day;
400 days. Outlets: seed `news_outlet.csv`. The raw feed copies keep the outlets' descriptions; they stay
in Landing/Archive as provenance (retention keeps what Bronze holds), see topics.md, "Storage".

## GDELT DOC 2.0 API (`gdelt`, topics)

**Implemented (10 Nov 2026, register CD-030; topics layer O32).** Per `gdelt_query` row of the seed
`topic_signal.csv` (an English query with `sourcecountry:PL`; GDELT searches machine-translated coverage), two
small JSON timelines a day from `https://api.gdeltproject.org/api/v2/doc/doc`: `mode=timelinevolraw` (matching
articles per day and all monitored articles, `norm`) and `mode=timelinetone`; 3 months (the API's window) on the
first landing, 2 weeks afterwards. Files are built by `ingestion/to_landing/gdelt/` from the seed and landed
through the shared `bulk_file` pattern. GDELT allows one request every 5 seconds and refuses shared GitHub runner
addresses for long stretches (HTTP 429, Dev probes 42784542844, 42785760450: one of six queries answered after
back-off), so the daily run is best effort: 6 s between requests, a different first topic each day, stop after 3
refused files in a row or 40 minutes; stale topics show in `gold.topic_quality`. Bronze `bronze.gdelt_timeline`
(accumulating), Silver `silver.topic_volume` (newest value per topic and day; volume_share = volume / norm).
Terms: unrestricted use with a citation of the GDELT Project and a link (gdeltproject.org/about.html).

## Wikimedia pageviews (`wikimedia_pageviews`, topics)

**Implemented (10 Nov 2026, register CD-025; topics layer O32).** Per `wiki_article` row of `topic_signal.csv`
(`<lang>:<Title>`), daily user views from the pageviews REST API
(`https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/<lang>.wikipedia.org/all-access/user/<Title>/daily/<start>/<end>`),
455 days on the first landing, 14 days afterwards, 0.2 s apart, with the platform's user agent (Wikimedia's
User-Agent policy). A 404 (renamed article) is a warning. Bronze `bronze.wiki_pageviews` (accumulating), Silver
`silver.topic_attention` (views summed over a topic's articles per day). Pageview data: CC0 1.0.

## Official release calendars (`release_calendars`, topics)

**Implemented (10 Nov 2026, register TP-010, TP-011, TP-014; NBP TP-012 and MF TP-013 parked; topics layer
O32).** Dev probes 42784902820, 42785648233, 42786020672, 42789309243. Eurostat: the documented ICS address
(`cache/RELEASE_CALENDAR/calendar_EN.ics`) answers 404; the euro-indicators calendar page loads its events from
`https://ec.europa.eu/eurostat/o/calendars/eventsJson?start=…&end=…&isEuroindicator=true` (JSON with title,
reference period, dataset codes), landed for 14 days back to 90 days ahead. GUS: no machine-readable calendar
(month and year calendars are HTML and PDF); the `kalendarium` day pages (`stat.gov.pl/kalendarium/DD-MM-YYYY.html`,
"Publikacje GUS" list: title and link), 7 days back to 45 ahead, 1 s apart. ECB: statistical calendar and
Governing Council meeting calendar (HTML; robots.txt Crawl-delay 5, respected). NBP (nbp.pl) answers automated
clients with a bot-protection page and www.gov.pl (MF) refuses connections from GitHub runners: both parked; the
seed `release_calendar.csv` stays the manual fallback. Files are built per run by
`ingestion/to_landing/release_calendars/`, landed through `bulk_file`; Bronze `bronze.release_calendar_items`
(newest copy of each file, latest-only); Silver `silver.releases` with `kpi_keys` (seed `release_kpi.csv` by
title, Eurostat also by dataset code against `kpi_v2_map`).

## MF local-government budget reports (`mf_jst_budgets`)

**Built (12 Nov 2026, register PLG-010).** gov.pl lists, per year, a page ("2025 r.", from
`https://www.gov.pl/web/finanse/sprawozdania-budzetowe`, paginated) whose sub-page "Zestawienia zbiorcze" links one
summary XLSX per quarter, unit type (Gminy, Miasta, Powiaty, Wojew, JST, Związki) and family (DW revenue and
expenditure, ZN liabilities and receivables): `<Type>_<DW|ZN>_<year>_k<quarter>_<YYYYMMDD>.xlsx`, from 2022 Q1 (204
files on 11 Nov 2026). Entity = type × family × year × quarter; a corrected file gets a new attachment URL and lands as
a new version; landed URLs are not downloaded again; restart unit: one file. Bronze `bronze.mf_jst_budgets_cells`
(one row per cell, shared `ingestion/common/xlsx_cells.py`). Silver `silver.mf_jst_budget_summary` (unit type ×
quarter × section × item × plan/executed/total, PLN, year to date). Gold block `jst_budget`: executed revenue,
expenditure, result and liabilities by unit type (KPIs `local_gov_revenue`, `local_gov_expenditure`,
`local_gov_balance`, `local_gov_debt`, draft). Weekly schedule (Wednesday). Licence: gov.pl terms (official
materials). The per-unit report databases (Rb27S, Rb28S, … ZIPs, 2001-2026, one row per unit and budget class) are a
separate, larger scope.
## MRPiPS registered unemployment (`mrpips_unemployment`)

**Built (11 Nov 2026, register PLS-029).** The Public Employment Services portal (psz.praca.gov.pl, "Bezrobocie
rejestrowane") publishes one ZIP per month, "statystyki strukturalne <month> <year>" (0.4-3 MB). Route: the listing
page links the newest ZIPs and, per year, a category page (`categoryId`) that links the year's asset page
("Statystyki strukturalne <year>"), which links the twelve ZIPs (`/documents/d/urzad-pracy/statystyki-strukturalne-…`
since 2023, `/documents/10828/<folder>/statystyki+strukturalne+….zip/<uuid>` before); months are read from the link
text. Entity = one month (`stat_YYYY_MM`), version = Last-Modified, a changed ZIP lands as a new version; restart
unit: one month. Each run lands the missing months and re-downloads the newest two (MRPiPS corrects recent rates);
older months are not downloaded again. Bronze `bronze.mrpips_unemployment_cells`: one row per cell (shared
`ingestion/common/xlsx_cells.py`) of the rate workbook ("Stopa bezrobocia rejestrowanego", until 2023
"stopa-pow-…": registered unemployed in thousands and the rate, Poland, voivodeships, powiats, TERYT WOJ./POW.)
and MRPiPS-01 sheets TAB 1 and TAB 2; a ZIP without a rate workbook in XLSX (January 2019 - August 2020: `.xls`
only) is rejected with that reason. Silver `silver.mrpips_registered_unemployment` (month x area x measure x sex):
`unemployed_thousands`, `unemployment_rate` (Poland, voivodeships, powiats) and `unemployed_persons` (TAB 2, total
and women, Poland and voivodeships). Gold block `registered_unemployment` (KPIs `registered_unemployed`,
`registered_unemployment_rate`, next to GUS BDL and DBW). Weekly schedule (Tuesday). Licence: CC BY 3.0 PL (portal
footer). The ZIPs since 2001, the half-yearly occupation and vacancy tables and the yearly MRPiPS-01 data files
(SPR1, about 27 MB) are not built yet.

## GUS publication tables: culture, participation in culture, tourism (`gus_culture`, `gus_culture_participation`, `gus_tourism`)

**Built (12 Nov 2026, register PLS-027, PLS-028, PLS-035).** stat.gov.pl publishes each edition of "Kultura i
dziedzictwo narodowe w <year> r." (until 2020 "Kultura w <year> roku"), "Uczestnictwo ludności w kulturze w <year> r."
(every five years) and "Turystyka w <year> r." on its own page with a tables file ("Tablice w formacie XLSX"): one
workbook or a ZIP of chapter workbooks. Route: the subject page links the newest edition, every edition page links the
others (`ingestion/common/gus_publications.py`, editions from 2019). Entity = one edition (`kultura_<year>`,
`uczestnictwo_<year>`, `turystyka_<year>`), version = download time (no Last-Modified) when the SHA-256 changed; the
newest edition is downloaded again on every run; restart unit: one edition. Bronze `bronze.<source>_cells` (every
cell of every workbook, with the ZIP member). Silver `silver.<source>_tables`: curated tables found by title and
column header (`warehouse/macros/gus_tables.sql`), never by cell position; GUS symbols kept as `cell_status` ('.'
and 'x' have no value, survey estimates marked 'v' are `low_reliability`). Tables: museum visitors, cinema
admissions, public library users and public libraries by voivodeship (culture); establishments and bed places
(31 July), tourists and overnight stays by voivodeship (tourism); visiting museums, using a library, attending
theatre in % of persons 15+, by sex (participation). Gold blocks `culture_institutions`, `accommodation`,
`participation` (annual); KPIs draft. Monthly schedule (6th). Licence: GUS terms (source given; commercial reuse not
addressed; no named licence, D23).

## GUS census tables: NSP 2021 and PSR 2020 (`gus_nsp2021`, `gus_psr2020`)

**Built (12 Nov 2026, register PLS-005, PLS-006).** One-off censuses: the final-results pages link one XLSX of tables
per theme at fixed `/download/` URLs, declared as files of the shared `bulk_file` pattern (no extractor code): NSP 2021
themes `ludnosc_stan_struktura`, `warunki_mieszkaniowe`, `rynek_pracy`; PSR 2020 `charakterystyka_cz2` (tables by
voivodeship). Entity = one workbook, version = download time when the SHA-256 changed, restart unit one file. Bronze
`bronze.<source>_cells` (`ingestion/common/gus_publications.py`); Silver `silver.<source>_tables` with the curated
tables found by title and header (`warehouse/macros/gus_tables.sql`): population 2011 and 2021 (total, urban, rural,
urban share), census activity, employment and unemployment rates; holdings 2010 and 2020, agricultural land, average
agricultural land per holding, cattle, pigs, by voivodeship. Gold blocks `census_2021`, `agricultural_census_2020`
(annual, the census year as period); census values are benchmarks and are not added to the population or unemployment
KPIs. Quarterly check. Licence: GUS terms (source given; commercial reuse not addressed).
