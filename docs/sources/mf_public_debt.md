# MF public finance sector debt (`mf_public_debt`)

**Extraction (2 Nov 2026).** The Ministry of Finance publishes the quarterly public finance
sector debt workbook (`Dlug_publiczny_<quarter>.xlsx`) as the second attachment of the stable page
`https://www.gov.pl/web/finanse/szeregiczasowe` (the first is the Treasury debt workbook); the page
`zadluzenie-sektora-finansow-publicznych` has only PDF reports. Each release is a new
`https://www.gov.pl/attachment/<uuid>`. The declaration `mf_public_debt.yaml` lands it through the
shared `bulk_file` pattern as entity `public_debt` (`url_from`, pattern `/attachment/`, `pick: last`;
`landing: inspect` lists every match). Version = Last-Modified (download time when absent); written
only when the SHA-256 changed. Restart unit: the workbook. Terms: gov.pl (register PLG-009). **Bronze to Gold (11 Nov 2026).** The workbook's cells go to `bronze.mf_public_debt_cells` (one row per non-empty cell: sheet, row, column, source text, number format; shared decoder `ingestion/common/xlsx_cells.py`, latest-only). Silver `silver.public_debt`: the six sheets in long form (`xlsx_wide_series`), unit `mln_pln` or `ratio` (the '%'-formatted debt-to-GDP rows). Gold: build stage `poc_build_public_finance.sql` (debt to GDP for State Treasury, PDP and general government EDP debt, quarterly and the 31 December value as the year (measure suffix `:YE`), which is the MF series of the v2 KPI `gg_debt_gdp` next to Eurostat (O31); the same debts and EDP debt by ESA instrument in million PLN). Schedule: Wednesdays 13:55 UTC. Dev probes 43084312730, 43085156243 reached www.gov.pl from Actions (an earlier refusal seen by the release-calendar probe did not recur); api.dane.gov.pl timed out from Actions, so it is not an alternative route.
