# EEA Datahub: greenhouse gas inventory (`eea`)

**Built (12 Nov 2026, register GL-037).** Every version of an EEA dataset is its own catalogue record with a public
file share. The GovReg inventory ("National emissions reported to the UNFCCC and to the EU under the Governance
Regulation") share holds the CSV as `UNFCCC_<n>_csv.zip` (8.6 MB, 100 MB unpacked, 1990 onwards), an SQLite file
(2.0 GB) and an Access database (198 MB). `ingestion/to_landing/eea/` resolves the newest version each run (the
series record's siblings → the version records of the newest year → each share's file list, public WebDAV) and
lands the ZIP through the shared `bulk_file` pattern; the share link sends no Last-Modified, so version = download
time and an unchanged ZIP is skipped. Entity: `govreg_ghg_inventory` (2026 ver. 2.1 in the trial: 692,640 rows, 33
areas, 9 pollutants, 166 sectors). Bronze: `bronze.eea_ghg_inventory` (CSV columns lower-cased, text). Silver:
`silver.eea_ghg_inventory` (grain country_code × format_name × pollutant_name × sector_code × year; value null when
the source gives a notation key such as NO, NE, IE, NA, C, never 0). Gold: `gold.build_eea` — all greenhouse gases
in Gg (= kt) CO2 equivalent, totals with and without LULUCF and CRF sectors 1–5, EU27 members (ISO alpha-2); the
EEA aggregate `EUA` is not mapped. KPI `ghg_emissions` (total without LULUCF, UNFCCC scope, so without
international aviation, unlike Eurostat). Refresh full (D32). Terms: CC BY 4.0 under the EEA legal notice, licence
row `cc_by_4_0_eea`. Restart unit: the dataset version.
