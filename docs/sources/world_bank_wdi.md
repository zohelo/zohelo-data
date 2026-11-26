# World Bank World Development Indicators (`world_bank_wdi`)

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
