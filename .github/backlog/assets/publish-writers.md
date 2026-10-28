# warehouse/publish.py writers (architect, 28 Oct 2026)

Source for the Tasks of Deliverable `data/published/published-v1/publish`. publish.py is the only writer of
`06_published/v1/` (decision D5); the files' fields are in docs/published.md, the schemas in
warehouse/published/schemas/.

## Shared parts (Task publish/kpi-latest creates them)

- `PREFIX = "06_published/v1/"`
- `LEVELS = {"country": "country", "wojewodztwo": "voivodeship"}`: Gold `geography_level` → contract level.
- `FREQ_RANK = {"D": 1, "B": 2, "W": 3, "M": 4, "Q": 5, "S": 6, "A": 7}`
- `SKIP = {"fx_rate": "one value per currency; latest.json has no currency field"}`
- `gold(cat, namespace, name)`: `cat.load_table((namespace, name)).scan().to_arrow()`; cat =
  `ingestion.common.r2.catalog()`.
- `seed(name)`: rows of `warehouse/seeds/<name>.csv` as dicts (csv.DictReader, utf-8). KPIs and their measures are
  read from the seeds in the repository (the same files dbt builds into gold.kpi and gold.kpi_measure).
- `iso(ts)`: a timestamp as `%Y-%m-%dT%H:%M:%SZ` in UTC.
- `period_label(date_key, frequency)`: A `2025`, S `2025-H1`/`2025-H2`, Q `2025-Q1`…, M `2025-03`, any other
  `2025-03-14`.
- `put(client, key, body: bytes)`: `client.put_object(Bucket=req("STORAGE_LAKEHOUSE_BUCKET_NAME"), Key=key,
  Body=body, ContentType="application/json", CacheControl="no-cache")`; returns `len(body)`.

## kpi/<kpi_key>/latest.json

`kpi_latest(metrics, measures, checked_at, kpi_key, unit_key, generated_at) -> dict` is pure (no R2): DuckDB over
the Arrow table `metrics` (columns of gold.metrics_<kpi_key>: source_system, measure_key, geography_key,
geography_level, date_key, frequency, value, observation_status_key).

1. Keep rows with `geography_level` in LEVELS, `value` not null and `geography_key <> '_X'`.
2. `priority` of a row = the smallest `priority` of the `measures` rows (kpi_measure.csv dicts) with this
   `kpi_key`, the row's `source_system` and `measure_key LIKE measure_key_pattern`; 999 when none matches.
3. Per `source_system` × `geography_key` keep one (measure_key, frequency): the smallest
   (FREQ_RANK[frequency], priority, measure_key).
4. Of its rows, the newest `date_key` gives `period` (period_label) and `value` (float); the next older gives
   `previous_period` and `previous_value`; both keys are left out when there is no older row.
5. `flags` = `[]` when observation_status_key is null or `ok`, else `[observation_status_key]`;
   `checked_at` = `checked_at.get(source_system, generated_at)`; `geography_level` = LEVELS[...];
   `source_key` = source_system.
6. `values` sorted by source_key, then country before voivodeship, then geography_key. Result:
   `{"kpi_key", "unit_key", "generated_at", "values"}`.

`checked(coverage) -> dict`: per `source` of gold.coverage, `iso(max(checked_at))`.

`main(prefix=PREFIX, namespace="gold") -> dict`: generated_at = iso(now); for every kpi.csv row with status
`published` and not in SKIP, read `<namespace>.metrics_<kpi_key>` (NoSuchTableError → the key goes to
`missing`), build kpi_latest with `seed("kpi_measure")` and `checked(gold(cat, "gold", "coverage"))`, encode with
`json.dumps(..., ensure_ascii=False, separators=(",", ":"))`, raise ValueError when the body exceeds 100,000 bytes
(contract size limit), put it at `prefix + f"kpi/{kpi_key}/latest.json"`. Annotate notice
`gold.publish: latest` with `{"written": {kpi_key: number of values}, "skipped": sorted(SKIP), "missing": [...]}`
(ingestion.common.report.annotate) and return the same dict. No `__main__` block and no activity entry: a later
Task wires `gold.publish` into warehouse.yaml, run.py and pipeline.yml.

## manifest object (warehouse/catalog.py, Task semantic-manifest/generate/manifest-object)

dbt writes the semantic models of warehouse/semantic_models/gold.yml (one per gold.metrics_<kpi_key>) to
`warehouse/target/semantic_manifest.json`: `semantic_models[]` with `node_relation.alias` (`metrics_population`
on prod, `poc_metrics_population` on other targets), `defaults.agg_time_dimension`, `dimensions[]` and
`entities[]` (each `{name, expr}`; expr may be null).

`manifest_object(semantic: dict, kpis: list[dict], generated_at: str) -> dict`, pure:

1. Index `semantic_models` by `node_relation.alias` with a leading `poc_` removed.
2. For each `kpis` row (kpi.csv dicts) with status `published`, in file order, whose `metrics_<kpi_key>` is in
   the index, add a metric:
   `kpi_key`; `table` = `gold.metrics_<kpi_key>`; `unit_key`; `frequency` = `frequency_default`;
   `time_column` = expr (else name) of the dimension whose name is `defaults.agg_time_dimension`;
   `value_column` = `value`; `source_column` = `source_system`;
   `dimensions` = `[{"dimension_key": "geography", "column": expr (else name)}]` for an entity named
   `poc_geography`, else `[]`.
3. Return `{"version": 1, "generated_at": generated_at, "metrics": [...]}`.

`manifest() -> dict`: reads `PROJECT / "target" / "semantic_manifest.json"` and `PROJECT / "seeds" / "kpi.csv"`
(csv.DictReader, utf-8); generated_at = now in UTC as `%Y-%m-%dT%H:%M:%SZ`; returns `manifest_object(...)`. It
writes nothing; publish.py writes manifest.json in a later Task.
