# Ministry of Justice ISWS justice statistics (`ms_isws`)

**Built (16 Nov 2026, register PLG-031, #2968).** The Ministry of Justice's statistical information service lists
its multi-year workbooks on https://isws.ms.gov.pl/pl/baza-statystyczna/opracowania-wieloletnie/ (88 on 16 Nov
2026) as `download,2853,<n>.html` links whose text is the title; `<n>` is the list position, so each landed
workbook is a curated entity matched by a regex on its title (`ingestion/to_landing/ms_isws/`, `SELECTION`): a
title that matches no link or several fails Landing. Entities: `caseload` (cases received, resolved and pending in
the common courts, 2011-), `duration` (average duration of court proceedings, one sheet per year, 2011-),
`adoptions` (2000-) and `notarial_acts` (1991-). Version = download time (no Last-Modified); every selected
workbook is downloaded on every run and lands only when its SHA-256 changed; restart unit one file. Bronze
`bronze.ms_isws_cells`: every non-empty cell (.xlsx through `ingestion/common/xlsx_cells.py`, .xls with xlrd).
Silver `silver.ms_isws_tables`: values found by row label and column header; '•' (no data) has no value. Gold block
`multi_year` (Poland, annual), registered in `warehouse/macros/sources/ms_isws.sql`; KPIs `court_cases_received`,
`court_cases_resolved`, `court_case_duration` (draft). Monthly check. Left out: the other 84 workbooks (convictions,
mediation, juveniles, family cases and others), court-level rows of the caseload table, the yearly PDF reports.
Licence: Ministry of Justice re-use terms (`ms_reuse_terms`: free re-use; state the source, the time of creation and
of obtaining the information, and any processing); isws.ms.gov.pl itself states no terms.
