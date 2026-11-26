# Eurostat (`eurostat`)

There is no single current archive for all ordinary Eurostat datasets. The supported
complete route is catalogue enumeration followed by one prepared, compressed full
file per inventory item. Eurostat explicitly warns that iterating the inventory can
start downloading the complete Eurostat data scope and require substantial disk.

## Complete discovery and metadata

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

## Full data files and structures

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
