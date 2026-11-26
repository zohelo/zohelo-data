# World Bank International Debt Statistics (IDS) (`world_bank_ids`)

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
