# MF local-government budget reports (`mf_jst_budgets`)

**Built (12 Nov 2026, register PLG-010).** gov.pl lists, per year, a page ("2025 r.", from
`https://www.gov.pl/web/finanse/sprawozdania-budzetowe`, paginated) whose sub-page "Zestawienia zbiorcze" links one
summary XLSX per quarter, unit type (Gminy, Miasta, Powiaty, Wojew, JST, Związki) and family (DW revenue and
expenditure, ZN liabilities and receivables): `<Type>_<DW|ZN>_<year>_k<quarter>_<YYYYMMDD>.xlsx`, from 2022 Q1 (204
files on 11 Nov 2026). Entity = type × family × year × quarter; a corrected file gets a new attachment URL and lands as
a new version; landed URLs are not downloaded again; restart unit: one file. Bronze `bronze.mf_jst_budgets_cells`
(one row per cell, shared `ingestion/common/xlsx_cells.py`). Silver `silver.mf_jst_budget_summary` (unit type ×
quarter × section × item × plan/executed/total, PLN, year to date). Gold block `jst_budget`: executed revenue,
expenditure, result and liabilities by unit type (KPIs `local_gov_revenue`, `local_gov_expenditure`,
`local_gov_balance`, `local_gov_debt`, draft). Weekly schedule (Wednesday). Licence: gov.pl terms (official
materials). The per-unit report databases (Rb27S, Rb28S, … ZIPs, 2001-2026, one row per unit and budget class) are a
separate, larger scope.

**Gold, full coverage (#3350).** Every value of `silver.mf_jst_budget_summary` by rule in `gold.mf_jst_budgets_*`
(`gold.observations`, D35; rule in [gold-coverage.md](../gold-coverage.md)): every item of each unit type's DW and ZN
workbook, plan and executed, Q, unit type as the breakdown. Dev trial 45971075181: 22,522 rows = Silver, 1,411 series,
334 measures, 0 duplicates by series_key × date_key; every row of the curated block found.
