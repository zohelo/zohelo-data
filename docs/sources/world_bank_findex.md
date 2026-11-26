# World Bank Global Findex Database (`world_bank_findex`)

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
