# KNF monthly banking-sector data (`knf_banking`)

**Extraction (26 Nov 2026, register PLG-017, #2871).** The Polish Financial Supervision Authority (KNF) replaces one
workbook in place on the stable page "Dane miesięczne sektora bankowego"
(`dane_statystyczne?articleId=56224`) about six weeks after each month; the file URL changes with every release
(`2026_09-Dane_statystyczne_PL_100679.xlsx`, 2.2 MB, data to September 2026, page updated 13 November 2026). The
declaration `knf_banking.yaml` finds the link on the page (`url_from`) and lands it through the shared `bulk_file`
pattern as entity `banking_monthly`. KNF sends no Last-Modified, so the version is the download time and a file is
written only when its SHA-256 changed; each release is the whole series since January 2018 (`snapshot`). robots.txt
of www.knf.gov.pl allows every agent (Dev probe 45758367857). Restart unit: the file. Not declared: the archive
workbook to December 2017 on the same page (another layout, before the IFRS 9 changes).

**Bronze to Gold.** Cells in `bronze.knf_banking_cells` (shared `xlsx_cells` decoder, latest-only). Silver
`silver.knf_banking_series`: every value, one row per sheet × bank group × item × period. A block starts at a date
header row (column 1 empty, date cells from column 2) and its bank group is the text right above it (banking sector,
commercial banks, branches of credit institutions, cooperative banks; commercial banks without associating banks on
the liquidity sheet). Items are identified by their path of headings and labels (leading spaces give the level),
never by row position; empty cells are absent, never 0. Gold (`poc_build_knf_banking.sql`, block `banking_sector`):
total assets, net profit year to date, gross non-financial receivables without and with impairment, non-financial
deposits and employees (monthly), the total capital ratio (quarterly, a fraction as stored); the whole sector is the
total, the bank groups the breakdown `bank_group`. Terms: the KNF site asks for the source and a link
("Przy wykorzystywaniu materiałów ze strony internetowej KNF wymagane jest podanie źródła wraz z linkiem do serwisu
www.knf.gov.pl."); reuse is not stated, so the licence is `unresolved` and the source stays internal. Schedule:
Thursdays 09:15 UTC (O46).

**Gold, full coverage (#3371).** Every value of `silver.knf_banking_series` by rule in `gold.knf_banking_*`
(`gold.observations`, D35; rule in [gold-coverage.md](../gold-coverage.md)): measure = sheet × item path × unit ×
frequency, the bank group as the breakdown (the whole sector is the total), Poland only, values and units as published
(`%` cells as stored fractions, unit `share`). Dev trial 46039337512 (Silver built in scratch from production Bronze):
193,909 rows = Silver, 1,940 series, 536 measures, 0 duplicates by series_key × date_key; every row of the curated
`build_knf_banking` (2,307) found with the same value.
