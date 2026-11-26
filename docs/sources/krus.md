# KRUS basic benefit statistics (`krus`)

**Extraction (2 Nov 2026).** KRUS publishes its basic beneficiary statistics on a stable gov.pl
page as one XLSX (about 20 kB) whose attachment URL changes with each update. The file is a
rolling series, not one quarter: the Dev trial (#2468) found one sheet `Świadczenia` (A1:U62) with
quarterly columns from I quarter 2020 to the newest quarter and yearly totals, for the average
monthly number of beneficiaries and the average monthly benefit in PLN: pensions and disability
pensions in all, old-age pensions, disability pensions (of which accident), family pensions,
parental supplementary benefits and benefits for civilian blind war victims. The declaration
`krus.yaml` finds the link on the page (`url_from`, the page's only `/attachment/` link) and lands
it through the shared `bulk_file` pattern as entity `swiadczenia_kwartalne`. The server sent no
Last-Modified in the trial, so the version is the download time and a file is written only when
its SHA-256 changed; each version is the whole series to date (`snapshot`). Older versions can hold quarters that a later file drops if KRUS shortens the window,
so `prune` stays `report`. Restart unit: the file. Not declared: the quarterly statistical
information PDFs (51 tables), which have no stable listing page. Terms: gov.pl, official materials
free to use (register PLS-011). **Bronze to Gold (11 Nov 2026).** Cells in `bronze.krus_cells`
(latest-only); Silver `silver.krus_benefits`: one row per period (quarter or year) × benefit type × measure
(`beneficiaries`, `avg_benefit_pln`), from I quarter 2020; benefit types from the header rows (group, then
"Ogółem" / "w tym wypadkowe"); an unknown column fails the `benefit_key` not-null test. The footnote mark
of III quarter 2025 on (survivors' pensions including those paid with other pensions) is kept in
`footnote` and becomes observation status `break` in Gold: a series break. Gold (`poc_build_public_finance.sql`): every benefit type and measure, quarterly
and yearly. Schedule: Wednesdays 14:05 UTC.
