# OECD Data Explorer (SDMX) (`oecd`)

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
