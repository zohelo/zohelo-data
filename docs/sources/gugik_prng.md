# GUGiK PRNG, state register of geographical names (`gugik_prng`)

**Built (12 Nov 2026, register PL-ENV-002, #2971).** Route: the national part of the register as two packages on
`opendata.geoportal.gov.pl/prng/` (localities `PRNG_MIEJSCOWOSCI_XLSX.zip`, physiographic objects
`PRNG_OBIEKTY_FIZJOGRAFICZNE_XLSX.zip`, about 23 MB each), the links the "Dane do pobrania" WMS GetFeatureInfo of the
PRNG page returns; declared as files of the shared `bulk_file` pattern (no extractor code). Entity = one package,
version = Last-Modified, written only when the SHA-256 changed; restart unit one file. Bronze `bronze.prng_names`
(`ingestion/to_bronze/gugik_prng/load.py`, DuckDB `read_xlsx`): one row per sheet row, every attribute as text, the
export date the package names; a package whose header, sheet or row count is not the declared layout is rejected
into `bronze.prng_rejected_files`. Silver `silver.prng_objects` (one row per PRNG identifier: names, status, category,
kind, SIMC id, territory and coordinates as published, decimal latitude and longitude of a main point) and
`silver.prng_object_gminas` (object × TERYT gmina code). Gold block `gold.build_gugik_prng`, registered in
`warehouse/macros/sources/gugik_prng.sql`: counts of named objects located wholly or partly in Poland, each
voivodeship, powiat and gmina (localities by kind, physiographic objects by category), daily at the export date;
KPI `localities` (draft). Monthly check (the 5th). Licence: "Dane są dostępne bezpłatnie i do dowolnego
wykorzystania" (PRNG page), attribution "Źródło: GUGiK, PRNG". Left out: the world-names part (`PRNG_SWIAT_*.zip`)
and the GML/SHP twins of the same records.
