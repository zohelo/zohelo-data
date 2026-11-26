# KNF pension-market statistics (`knf_pensions`)

**Extraction (26 Nov 2026, register PLG-018, #2871).** Every KNF statistics release is its own article of
`https://www.knf.gov.pl/publikacje_i_opracowania/dane_statystyczne`, so the article and the file URL change with each
release. `ingestion/to_landing/knf_pensions/` reads the PPK category page (`/dane_statystyczne/PPK`; the main listing
when it holds no PPK article), takes the newest quarterly release "Wartość aktywów netto funduszy zdefiniowanej daty
(PPK) wg stanu na koniec <quarter> kwartału <year>" by the quarter in its title, and lands its one XLSX
(`www_Art_100435.49_UoPPK_2026_3Q_100435.xlsx`, 130 kB, 30 October 2026) as entity `ppk_quarterly`; more than one
XLSX on the article stops the run instead of picking one. The workbook is a rolling series since 2020 Q1, so only the
newest release is landed (`snapshot`); version = download time (no Last-Modified), written only when the SHA-256
changed. Restart unit: the file. Not declared yet: monthly OFE data, the OFE and insurance quarterly bulletins, IKE and
IKZE (one file per period each).

**Bronze to Gold.** Cells in `bronze.knf_pensions_cells` (shared `xlsx_cells` decoder, latest-only). Silver
`silver.knf_pensions_ppk_net_assets`: Tabela 2, net asset value of the PPK target-date funds by managing institution
and the total, thousand PLN as published, per quarter; header found by its label, years carried from their merged
header cells, rows to the 'Suma' row; '-' is `no_value` (never 0). Gold (`poc_build_knf_pensions.sql`, block
`ppk_net_assets`): one quarterly measure, the total and the managing institutions (breakdown `manager`). Terms as for
`knf_banking`: source and link required, reuse not stated, licence `unresolved`, internal. Schedule: Fridays 10:25 UTC
(O46).
