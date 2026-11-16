# GPW EBI issuer reports, GPW report list (`gpw_ebi`)

**Built (16 Nov 2026, register CD-007, #1488).** Route (decision D37): GPW's public list
`https://www.gpw.pl/espi-ebi-reports` through the page's own "Show more" request (the form posted to
`https://www.gpw.pl/ajaxindex.php`, only "EBI" ticked, 15 rows a request, newest first), one request at a time 5 s
apart, the platform's User-Agent, at most 3 tries; robots.txt allows it; a 4xx stops the run.
`https://www.gpw.pl/ebi` answers 403 to automated clients and is not used. ESPI rows of the same list are CD-006
(`knf_espi`). Entity: one day of publication (`day_<YYYYMMDD>`), landed as one reduced JSON document (code:
`ingestion/to_landing/gpw_ebi/` on `ingestion/common/report_lists.py`). Version: download time; a day is written
again only when its SHA-256 changed. Each run reads from the top down to the newest held day (or 3 days back); the
first run reads back to 1 January of the previous year. Restart unit: one run's pass (nothing lands if it fails).

Stored per report: geru_id, publication time (Warsaw, as listed), category, report number, issuer name and ISIN
(where listed), link to the report's page. **Left out:** titles (they can name natural persons), report texts and
attachments, the prices shown beside each row; KRS, NIP, REGON and LEI are not in the list. In EBI items the issuer
link's text starts with the title (Dev trial 43831909084): the title is cut off in memory before Landing, and an item
whose link still holds the title is rejected as an unknown layout.

Bronze `bronze.gpw_ebi_reports` (one row per report, text). Silver `silver.gpw_ebi_reports` (grain `report_id`;
`tests/gpw_ebi_reports_complete.sql`: every held day and report reaches Silver, none misfiled). Gold
`gold.filing_gpw_ebi` (one row per report, ISIN as the security key) and `gold.build_gpw_ebi` (block `reports`:
reports per month of publication, Poland, total and by category, complete months only, no row (never a 0) for a
month without a report held; measure `gpw_ebi:reports:reports`, unit `reports`). Terms: none stated for the list;
licence `unresolved`, internal only. Refresh full (D32), daily 04:55 UTC.
