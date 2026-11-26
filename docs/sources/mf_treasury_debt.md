# MF State Treasury debt (time series) (`mf_treasury_debt`)

**Extraction (2 Nov 2026).** The Ministry of Finance publishes the State Treasury debt time
series as an XLSX workbook linked from the stable page `https://www.gov.pl/web/finanse/szeregiczasowe`;
each release is a new `https://www.gov.pl/attachment/<uuid>`. The declaration
`mf_treasury_debt.yaml` lands it through the shared `bulk_file` pattern as entity
`treasury_debt_series`, finding the link on the page at run time (`url_from`, pattern
`/attachment/`; `landing: inspect` lists every match). Version = Last-Modified (download time
when absent); written only when the SHA-256 changed. Restart unit: the whole workbook. Terms:
gov.pl (register PLG-008). **Bronze to Gold (11 Nov 2026).** The workbook's cells go to `bronze.mf_treasury_debt_cells` (one row per non-empty cell: sheet, row, column, source text, number format; shared decoder `ingestion/common/xlsx_cells.py`, latest-only). Silver `silver.treasury_debt`: every time-series sheet in long form (macro `xlsx_wide_series`: a block starts at a row of dates, items are the column-1 labels below it; repeated labels such as currencies are told apart by `item_occurrence`), month ends from 2001. Gold: build stage `poc_build_public_finance.sql` (debt by instrument, million PLN, monthly). Schedule: weekdays 13:45 UTC. Dev probes 43084312730, 43085156243 reached www.gov.pl from Actions (an earlier refusal seen by the release-calendar probe did not recur); api.dane.gov.pl timed out from Actions, so it is not an alternative route.
