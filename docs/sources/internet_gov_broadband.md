# internet.gov.pl fixed broadband coverage report (`internet_gov_broadband`)

**Built (13 Nov 2026, register PL-ENV-034, #2963).** The Ministry of Digital Affairs publishes the quarterly report
"Polska w zasięgu stacjonarnego dostępu do internetu" (data of internet.gov.pl) as one release page per edition on
gov.pl/web/cyfryzacja with 17 PDF attachments. Only the national PDF (about 1.4 MB) is landed. The release pages have
no stable index, so the declaration holds a seed list, one row per quarter, Q2 2024 to Q3 2026 (all ten editions,
found on the ministry's news list by Dev probe 43323654663). A quarter that ended more than 20 days ago and has no
seed row raises the warning "edition expected but not in the seed list" in landing.inspect and landing.extract. The
row is added by hand: nothing is guessed.
Landing: shared `bulk_file` pattern. Entity = one edition (`polska_<year>q<n>`), version = the attachment's
Last-Modified, written only when the SHA-256 changes. Restart unit: one edition.
Bronze `bronze.internet_gov_broadband_cells`: the PDF text layer read with pypdf (layout mode, `requirements.txt`),
one row per value cell as printed, with table, page, row label and column header. Three tables are kept, each found
by its title and checked before a cell is kept: header, the closed list of row labels, cells per row, number formats,
SUMA rows, shares against their counts, and the printed reference date against the edition's quarter. A file that
fails any check goes to `bronze.internet_gov_broadband_rejected_files` with the reason. "0" and "0,00%" are printed
zeros. The report has no suppression or missing marker; a missing cell rejects the file.
Silver `silver.internet_gov_broadband_coverage`: one row per reference date x table x area x class, with typed
counts and the printed shares (per cent, with their denominator in `address_points_share_basis`).
Gold block `coverage` (quarterly): address points in coverage and all address points (Poland and voivodeships);
for Poland, at least 100 Mb/s and at least 1 Gb/s (sums of the bandwidth classes), and the kinds of coverage with
their printed shares of all address points. No share is calculated. KPIs (draft): `broadband_coverage_points`,
`broadband_100mbps_points`, `broadband_gigabit_points`, `broadband_no_coverage_share`. Monthly check on the 20th.
Licence: `unresolved` (gov.pl terms say nothing about attached files), so the source stays internal.
Left out: the report's tables by medium, kind of service and investment plans; the voivodeship PDFs (about 255 MB per
edition); the UKE annual market report.
