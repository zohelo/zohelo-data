# MF state budget execution (monthly) (`mf_state_budget`)

**Extraction (2 Nov 2026; one entity per year since 11 Nov).** The Ministry of Finance publishes the
monthly operational report on state budget execution on per-year pages
`https://www.gov.pl/web/finanse/sprawozdania-miesieczne-<year>`, as xlsx/pdf pairs, newest first (pages
2018–2026 list XLSX; 2010–2017 list none); each release is a new `https://www.gov.pl/attachment/<uuid>`.
Table 1 of every report repeats each month of its year so far (cumulative from January), so the newest
report of each year holds the whole year. `mf_state_budget.yaml` declares the file once with
`years: {from: 2018}`: the `bulk_file` pattern expands it to entities `budget_execution_2018` …
`budget_execution_<current year>` (`url_from` page with `{year}`, `pick: first`); the current year's entity is
optional, so January, before the first report, is not a failure, and the yearly rollover needs no code
change. With `years.open: 2` only the current and the previous year are downloaded again; older years
land once (their December report is final). Version = Last-Modified; written only when the SHA-256 changed. Terms: gov.pl (register PLG-007).
**Bronze to Gold (11 Nov 2026).** Cells in `bronze.mf_state_budget_cells` (latest-only per year entity);
Silver `silver.budget_execution`: table 1, one row per year × month × item × measure (`execution_ytd`
cumulative, `plan` = budget act), thousand PLN, `item_key` for revenue, expenditure, balance, EU funds
result and financing (domestic, foreign). Gold (`poc_build_public_finance.sql`): execution monthly and the
December value as the year (measure suffix `:A`, so it never shares a series × date with January), plan yearly. Tables 2–20 are in Bronze, not yet modelled. Schedule: weekdays
13:35 UTC.
