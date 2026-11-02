# Publishing a draft KPI (product owner, 2 Nov 2026)

Source for the `data/metadata/catalogue-v1/kpis-40/publish-*` Tasks. A KPI whose blocks are active already has its
measures in gold.concept_measures; publishing it adds a metrics model and the catalogue entries that `repo_check`
and `gold.publish` read. Each Task names its KPIs (`<k>`), the semantic aggregation, the value-range bounds and the
fixture values; everything else is this recipe.

## Steps (per KPI `<k>`)

1. `warehouse/models/gold/poc_metrics_<k>.sql`: the three lines of `poc_metrics_unemployment_rate.sql` with
   `<k>` (alias `metrics_<k>`, the comment, `poc_concept_metrics('<k>')`).
2. `warehouse/warehouse.yaml`: add `gold.metrics_<k>` to the `gold:` list after the last `gold.metrics_*` entry;
   add `poc_metrics_<k>: {rows: <ceiling>}` after the last `poc_metrics_*` ceiling. Ceiling = 2 × the trial's
   rows for `<k>`, rounded up to the next 10,000.
3. `warehouse/semantic_models/gold.yml`: a semantic model `poc_<k>` shaped like `poc_unemployment` on
   `ref('poc_metrics_<k>')` (description: the KPI's name_en and `(gold.metrics_<k>)`; entity `<k>_row`), whose one
   measure is named and aggregated as the Task says; then a metric `poc_<k>` (type simple, label name_en,
   one-sentence description) at the end of `metrics:`.
4. `warehouse/tests/metrics_value_range.sql`: the Task's bounds rows at the end of `bounds`; the model in the
   `metrics` union, shaped like the other members.
5. `warehouse/seeds/kpi.csv` and the status table of `.github/backlog/assets/catalogue-seeds.md`: `published`.
6. `warehouse/published/fixtures/catalog/catalog.json`: a `kpis[]` entry shaped like `population`, with the KPI's
   kpi.csv fields, `source_keys` and `geo_levels` as the Task says, periods `1990`–`2024`, status `published`.
7. `warehouse/published/fixtures/manifest.json`: a `metrics[]` entry shaped like `inflation`, with
   `gold.metrics_<k>`, the KPI's unit_key and frequency_default.
8. `warehouse/published/fixtures/kpi/<k>/latest.json`: the Task's values (synthetic, D26), in the layout of
   `fixtures/kpi/unemployment_rate/latest.json` (key order, 2-space indent, `checked_at` 2026-01-01T00:00:00Z); each
   value's `unit_key` is its kpi_measure unit_key, or the KPI's when that is empty. One row per value in the table of
   `.github/backlog/assets/latest-fixtures.md`.
9. `docs/model.md`: add `metrics_<k>` to the list of published metrics tables.

## Trial (Dev · branch runs)

`dev/<task slug>_trial.py`: read `gold.fact_annual`, `gold.fact_monthly`, `gold.concept_measures` and
`gold.dim_series` with `ingestion.common.r2` `catalog()` (`load_table(...).scan().to_arrow()`) into duckdb; count
the rows of facts ⨝ concept_measures (concept_key = `<k>`) ⨝ dim_series (`is_total`) per KPI and source_system;
annotate one notice `metrics rows` with `{<k>: {source_system: rows}}`. It writes nothing. Run it on the branch,
then delete `dev/` before the PR. A KPI × source of the Task with 0 rows: comment the notice on the issue and stop.

## After the merge

The next Pipeline `silver/gold: build models` and `gold: publish` runs are green and the publish annotation lists
the KPIs under `written`. The zohelo and zohelo-bi fixture copies follow in their own Tasks.
