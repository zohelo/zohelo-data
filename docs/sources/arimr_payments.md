# ARiMR direct-payment statistics (`arimr_payments`)

**Built (Nov 2026, register PLS-034, #2487).** ARiMR publishes its programme statistics only as PDF attachments of
the gov.pl scheme pages linked from "Wdrażane programy i działania - dane liczbowe" (no XLSX or CSV; Dev probe
43343134150). The page https://www.gov.pl/web/arimr/platnosci-bezposrednie2 lists one report a month, "Dane wg stanu na
<date>" (December 2017 to the newest month), Excel sheets printed to PDF. Each report restates every campaign
cumulatively as of its date, so Landing takes the newest report and every year-end (31 December) report, which also
hold applications and decisions back to 2004 (ten files, about 3 MB). Every year-end report is landed, not only the
newest, because Bronze rejects a whole file it cannot check: the 31.12.2025 report prints "Zachodniopomorskie" twice
and no "Wielkopolskie" in its 2004-2016 applications table (Dev run 45747874758), so those campaigns come from the
31.12.2024 report. Entity = one report (`report_<yyyymmdd>`), version = download
time, a landed attachment URL is not downloaded again, restart unit one file (`ingestion/to_landing/arimr_payments/`);
two files for one report date, or no year-end report on the page, fail Landing.

Bronze `bronze.arimr_payments_cells` reads the PDF text layer with pypdf (layout mode, `requirements.txt`) into one
row per table cell: title, "Dane na dzień" date, column header as printed (campaign year and footnote marks), row
label, value as printed. A table is accepted only when its header names one increasing campaign year per value, its
rows are the 16 voivodeships and "Razem" once each, every value is a number as printed, and "Razem" equals the sum of
the voivodeships in every column (counts exactly, amounts printed rounded to the złoty within 9 zł); any other
layout rejects the whole file into `bronze.arimr_payments_rejected_files`. Footer lines, including the names of the
ARiMR staff responsible for the release, stay in the Landing file only.

Silver `silver.arimr_payments_direct_payments`: applications for area payments submitted, decisions granting them
and direct payments paid (PLN; JPO, UPO and the other direct-payment schemes, from 2023 with the CAP Strategic Plan
payments) by campaign and voivodeship, "Razem" = Poland; the newest report holding a campaign wins. Applications for
2025 on count unique producers who applied (footnote "Raport DPB"); the footnote marks stay in `column_note`.
Gold block `direct_payments` (annual, date = 1 January of the campaign; a campaign whose payment window to 30 June of
the next year was still open at the report date is `provisional`), registered in
`warehouse/macros/sources/arimr_payments.sql`; KPI `direct_payments_paid` (draft, million PLN in metrics); value
bound in `warehouse/macros/value_range/arimr_payments.sql` (D37). Schedule: one run a month on the 2nd, 05:45 UTC
(the month's report appears about the 25th to 28th of the next month); the run ends with Bronze, and Silver and Gold
are built by the platform's weekly run on Sunday (O46). Licence: gov.pl terms plus the report's own line "Wykorzystanie danych możliwe za podaniem źródła"
(`gov_pl_terms_arimr`, attribution required, share-alike applied as for gov.pl text). Not modelled: the PS WPR
2023-2027 intervention reports, national aid, RDP, KPO, fisheries and market-mechanism pages.
