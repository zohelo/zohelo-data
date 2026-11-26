# World Bank Worldwide Governance Indicators (WGI) (`world_bank_wgi`)

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
