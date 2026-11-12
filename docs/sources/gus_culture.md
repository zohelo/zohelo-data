# GUS culture and national heritage tables (`gus_culture`)

**Built (12 Nov 2026, register PLS-027, #2893).** stat.gov.pl publishes each edition of "Kultura i dziedzictwo
narodowe w <year> r." (until 2020 "Kultura w <year> roku") on its own page with a tables file ("Tablice w formacie
XLSX": one workbook in 2025, a ZIP of chapter workbooks before). Route: the subject page links the newest edition,
every edition page links the others (`ingestion/common/gus_publications.py`, editions from 2019). Entity = one edition
(`kultura_<year>`), version = download time (no Last-Modified) when the SHA-256 changed; the newest edition is
downloaded again on every run; restart unit: one edition. Bronze `bronze.gus_culture_cells` (every cell of every
workbook, with the ZIP member). Silver `silver.gus_culture_tables`: museum visitors, cinema admissions, cinemas,
public library users and public libraries by voivodeship, found by title and column header
(`warehouse/macros/gus_tables.sql`), never by cell position; GUS symbols kept as `cell_status`. Gold block
`culture_institutions` (annual), registered in `warehouse/macros/sources/gus_culture.sql`; KPIs `museum_visitors`,
`cinema_admissions`, `public_library_users` (draft). Monthly schedule (6th). Licence: GUS terms (source given;
commercial reuse not addressed; no named licence, D23).
