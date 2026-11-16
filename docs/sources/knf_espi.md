# KNF ESPI issuer reports, OAM list (`knf_espi`)

**Built (16 Nov 2026, register CD-006, #1486).** Route (decision D36): KNF's public OAM list of regulated information,
`https://moam.knf.gov.pl/moam.nsf`, its own view by day of receipt (`mOAM?readForm&RestrictToCategory=<day>`, pager
of 20), list pages only, one request at a time 5 s apart, the platform's User-Agent, at most 3 tries; robots.txt is
404; a 4xx stops the run. Never a report page or file. Entity: one day of receipt (`day_<YYYYMMDD>`), landed as one
reduced JSON document (code: `ingestion/to_landing/knf_espi/` on `ingestion/common/report_lists.py`). Version:
download time; a day is written again only when its SHA-256 changed. Each run reads today and the 3 days before,
working days a past run failed on, and, until the history is held, every day back to 1 January of the previous year
(newest first, 45 minutes a run). Restart unit: one day.

Stored per report: OAM document id, issuer name, day received, the ESPI form code that starts the listed title
(`RB`, `RB-W`, `QSr`, `UNI-EN`, …; `_U` when none), link to the report's page, the day's stated count. **Left out:**
titles (they can name natural persons: MAR art. 19 notices, board changes), report texts and attachments, issuer
identifiers (the list gives none: no KRS, NIP, REGON, LEI or ISIN, so no join to GLEIF; names are never matched).

Bronze `bronze.knf_espi_reports` (one row per report, text). Silver `silver.knf_espi_reports` (grain `report_id`;
`tests/knf_espi_reports_complete.sql`: every day holds the count the list states). Gold `gold.filing_knf_espi`
(one row per report) and `gold.build_knf_espi` (block `reports`: reports per month of receipt, Poland, total and by
form code, complete months only; measure `knf_espi:reports:reports`, unit `reports`). Terms: none stated for the
OAM; licence `unresolved`, internal only. Refresh full (D32), daily 04:40 UTC.
