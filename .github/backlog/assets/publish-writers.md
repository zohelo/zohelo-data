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

## Writers added after latest.json (architect, 28 Oct 2026, second batch)

Each writer below is a function of warehouse/publish.py that main() calls once, after the latest.json loop and
before the annotate call; its return value goes into the annotated dict under the key named in its section (the
annotation title stays `gold.publish: latest`). `cat`, `client` and `prefix` are the catalog, the S3 client and the
prefix main() already holds. When put() has no `content_type` keyword yet, the first writer that needs it adds
`content_type: str = "application/json"` and passes it as `ContentType`.

`parquet(table) -> bytes`: `pyarrow.parquet.write_table(table, buffer, compression="zstd")` into an `io.BytesIO`,
returns its bytes (shared by series and catalog tables; the first Task that needs it adds it).

## kpi/<kpi_key>/series.parquet — key "series"

`SERIES_LEVELS = {"country": "country", "wojewodztwo": "voivodeship", "powiat": "powiat", "gmina": "gmina",
"makroregion": "nuts1", "region": "nuts2", "nuts1": "nuts1", "nuts2": "nuts2"}`: Gold `geography_level` →
contract level (Polish NUTS 1 = makroregion, NUTS 2 = region).

`kpi_series(metrics, kpi_key, unit_key) -> pa.Table` is pure: DuckDB over the Arrow table `metrics` (columns of
the latest.json section plus `breakdown_json`); fetch with `.to_arrow_table()` (duckdb 2.0 has no
`fetch_arrow_table`).

1. Keep rows with `value` not null, `geography_key <> '_X'`, and either `geography_key = '_Z'` (not
   geographic, e.g. exchange rates: `geography_level` = `'_Z'`) or a `geography_level` in SERIES_LEVELS (mapped).
2. Extra dimensions: each key of `json_keys(breakdown_json)` over the kept rows whose value
   (`json_extract_string`, `'_T'` when the row lacks the key) takes more than one distinct value in the table. Column
   `dim_<key>` (key lower-cased, any character other than a–z, 0–9, `_` replaced by `_`). A key with one value
   is not a dimension.
3. Columns in this order: kpi_key (argument), source_key (= source_system), measure_key, geography_key,
   geography_level, period (period_label in SQL: A `%Y`; S `%Y-H1` when month ≤ 6 else `%Y-H2`; Q `%Y-Q<quarter>`;
   M `%Y-%m`; other `%Y-%m-%d`), period_start (date_key as date), frequency, value (double), unit_key (argument),
   flags (`''` when observation_status_key is null or `ok`, else observation_status_key), then the dim_ columns
   in key order.
4. Sorted by source_key, measure_key, geography_key, period_start.

main(): every kpi.csv row with status `published` gets its metrics table read (SKIP applies to latest.json only, so
fx_rate gets a series); `parquet(kpi_series(...))` is put at `prefix + f"kpi/{kpi_key}/series.parquet"` with
content type `application/vnd.apache.parquet`; ValueError when a file exceeds 20,000,000 bytes (contract limit).
Annotated value: `{kpi_key: rows}`.

## catalog/<table>.parquet — key "catalog_tables"

`CATALOG_TABLES = {"dim_kpi": "kpi", "kpi_measure": "kpi_measure", "kpi_dimension": "kpi_dimension",
"dim_domain": "domain", "dim_unit": "unit", "dim_source": "dim_source", "dim_licence": "licence",
"freshness": "freshness"}`: contract file name → Gold table (seeds kpi, kpi_measure, domain, unit, licence;
models kpi_dimension, dim_source; freshness is not built yet).

`catalog_tables(cat, client, prefix, namespace) -> dict`: in this order, `gold(cat, namespace, table)`
(NoSuchTableError → the file name goes to `missing`), `parquet(...)` put at `prefix + f"catalog/{name}.parquet"`
(content type `application/vnd.apache.parquet`). Returns `{"written": {name: rows}, "missing": [...]}`.

## geo/country.geojson and geo/voivodeship.geojson — key "geo"

`GEO = {"kraj": "country", "wojewodztwo": "voivodeship"}` (silver.prg_units `unit_level` → contract level);
`GEO_TOLERANCE = 0.005` degrees (about 350–550 m). PRG geometry is WKB in geographic ETRS89 (x = longitude,
y = latitude), which GeoJSON takes as is.

`features(shapes, names, level) -> dict` is pure: `shapes` = list of `{"geography_key", "geometry"}` (geometry a
GeoJSON dict), `names` = `{geography_key: {"name_pl", "name_en"}}`. Sorted by geography_key, one Feature each:
properties geography_key, name_pl, name_en (name_pl when name_en is null), level, and `parent_key` `"PL"` unless
the level is country; geometry type as given, every coordinate rounded to 4 decimals. A key missing from `names`
raises ValueError. Returns `{"type": "FeatureCollection", "features": [...]}`.

`geo(cat, client, prefix) -> dict`:

1. `cat.load_table(("silver", "prg_units")).scan(row_filter=In("unit_level", list(GEO)),
   selected_fields=("unit_level", "teryt_code", "geometry_wkb")).to_arrow()` (`from pyiceberg.expressions import In`).
2. Names: `cat.load_table(("gold", "dim_geography")).scan(row_filter=In("level_name", ["country", "wojewodztwo"]),
   selected_fields=("geography_key", "name_pl", "name_en")).to_arrow()`.
3. DuckDB (`INSTALL spatial; LOAD spatial;`) over the PRG rows: geography_key `'PL'` for kraj, else
   `'TERC:' || teryt_code`; geometry `ST_AsGeoJSON(ST_SimplifyPreserveTopology(ST_GeomFromWKB(geometry_wkb),
   GEO_TOLERANCE))`, parsed with json.loads.
4. ValueError when a geography_key occurs twice, or when there is not exactly 1 country and 16 voivodeships.
5. Per level: `features(...)`, `json.dumps(..., ensure_ascii=False, separators=(",", ":"))`, ValueError over
   2,000,000 bytes, put at `prefix + f"geo/{level}.geojson"` with content type `application/geo+json`.
6. Returns `{level: number of features}`.

## gold.publish activity (run.py, warehouse.yaml, pipeline.yml)

Job for pipeline.yml, placed between `silver-gold-build` and `serving-refresh-index`:

```yaml
  gold-publish:
    needs: [plan, silver-gold-build]
    if: "${{ !cancelled() && !failure() && contains(fromJSON(needs.plan.outputs.activities), 'gold.publish') }}"
    name: gold.publish
    # warehouse/publish.py is the only writer of 06_published/v1/ (docs/published.md): one at a time; a newer
    # queued publish replaces an older queued one (cancelled by design, not an alert).
    concurrency:
      group: zohelo-pipeline-publish
      cancel-in-progress: false
    runs-on: ubuntu-24.04
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1  # v7.0.1
        with:
          persist-credentials: false
      - uses: ./.github/actions/activity
        with:
          source: ${{ inputs.source }}
          activity: gold.publish
```

Changes (Task publish/activity):

1. run.py: `"gold.publish"` in ORDER after `"silver_gold.build"`, and in WRITES; in RUNS, `"gold.publish"` after
   `"silver_gold.build"` in the full dataflow and in `"silver/gold: build models"`; new run `"gold: publish":
   ["gold.publish"]` after that one; PLATFORM_ACTIVITIES `"gold.publish": _publish`, where `_publish(p: Params) ->
   int` imports `warehouse.publish`, calls `main()` and returns 0; the module docstring names gold.publish with the
   other platform activities.
2. run.py plan(): the line that removes silver_gold.build from a full dataflow when the platform does not authorize
   it does the same for gold.publish (loop over both).
3. warehouse/warehouse.yaml: `activities: [silver_gold.build, gold.publish]`; header comment line
   `#   gold.publish          writes 06_published/v1/ from Gold (warehouse/publish.py)`.
4. pipeline.yml: run option `"gold: publish"` after `"silver/gold: build models"`; the job above;
   serving-refresh-index `needs: [plan, gold-publish]`; notify `needs` gains gold-publish after silver-gold-build,
   and its `if` gets no cancelled condition for it (like silver-gold-build).
5. ops/notify_failure.py: `CANCELLED_BY_DESIGN = ("silver_gold.build", "gold.publish")`.
6. docs/operating.md: table row after silver_gold.build ``| `gold.publish` | `warehouse/publish.py` | platform:
   rewrites 06_published/v1/ from Gold (docs/published.md) |``; in the full-dataflow paragraph, "then the published
   files (gold.publish)" before the portal index.
