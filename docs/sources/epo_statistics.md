# EPO patent statistics (`epo_statistics`)

**Built (27 Nov 2026, register EX-004, #3320; waiting for the scheduled runs).** The European Patent Office's statistics
data-download page (https://www.epo.org/en/about-us/statistics/data-download) links about 40 XLSX workbooks of the
current EPO Technology Dashboard edition on link.epo.org, every file name ending `-epo-technology-dashboard-<edition>.xlsx`
(Dev probe 45924424161: 36 workbooks of the 2025 edition, 18-173 KB, status date 2 Feb 2026, no Last-Modified or ETag;
robots.txt of www.epo.org allows the page, link.epo.org has none). Open Patent Services is not used (OAuth credentials;
its robots.txt answered 403).

Route: `ingestion/to_landing/epo_statistics/` reads the page once a run and matches seven workbooks by file name
(`SELECTION`), each landed through the shared bulk_file pattern (exact bytes, receipt after the file, a copy lands only
when its SHA-256 changed). Entity = `<table>_<edition>` (`applications_by_country_2025`, `grants_by_country_2025`,
`applications_by_field_2025`, `grants_by_field_2025`, `applications_by_field_country_2025`,
`grants_by_field_country_2025`, `poland_2025`), so an edition keeps its files when the next one replaces it on the page.
Version = download time; restart unit: one file; one request at a time, 2 s apart. A selected table that matches no link
fails inspect and extract. Bronze `bronze.epo_statistics_cells` (every non-empty cell, `ingestion/common/xlsx_cells.py`);
a workbook whose sheets are not its table's layout goes to `bronze.epo_statistics_rejected_files`.

Silver `silver.epo_statistics_counts`: one row per edition x table x area x field x year, read by header text: the
country tables (ISO 3166-1 alpha-2 codes as published, 2016-2025), the field tables (35 WIPO IPC concordance fields and
'Unclassified', all origins), the field x country tables (one sheet per year, 47 countries named in the header, 'Other
countries', 'Total'; each year sheet keeps its own status date, e.g. 27 Jan 2020 for 2016) and the Poland workbook's
NUTS 2 sheet "Provinces" (2024 and 2025). Counts as published; residual rows and columns kept. The test
`warehouse/tests/epo_statistics_counts_complete.sql` checks that each table adds up to its totals. Dev trial 45930460266: 7 files (386,557 bytes), 41,963 Bronze
cells, Silver 38,445 rows, Gold 36,995 rows, 0 duplicate keys; only MQ (Martinique, no Gold country) maps to `_X`.

Gold `build_epo_statistics` (blocks `by_country`, `by_field`, `by_field_country`, `regions`, registered in
`warehouse/macros/sources/epo_statistics.sql`): editions overlap in years, so the newest edition holding a year wins.
Countries by ISO2 code or by the printed name, 'Total' = the world (WB:WLD), Polish NUTS 2 regions to their TERC
voivodeship, NUTS:PL91 (Warszawski stołeczny) and NUTS:PL92 ('Mazowieckie' in the sheet). KPIs
`european_patent_applications` and `european_patents_granted` (draft, by country). Yearly check (22 April, 07:25 UTC),
after the EPO's spring release.

Not built yet: the dashboard summaries (EU and EPO-state breakdowns, applications per million inhabitants), the
top-applicant lists (company and university names), the top-10 tables, the other 19 country workbooks (regions of
their countries) and the national-office files; record-level EP data from Linked open EP data (CC BY 4.0) is a later
step. Not built in Gold: 'Not classified' countries, 'Other countries', the regions' 'Total' and 'Unclassified'.
Overlap: WIPO IP statistics (`wipo_ip_stats`) and UPRP statistics (`uprp_statistics`) count other filings; each is
its own series (O31).

Licence: `unresolved` (internal). The data-download page states no reuse terms, and the EPO's terms-of-use and legal
notice pages gave the probe no reuse text (register notes, read 26 Nov 2026); owner question in the register.
