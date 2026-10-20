# Warehouse

The dbt project (dbt-duckdb, R2 Data Catalog attached as `r2`): Bronze → Silver → Gold,
the dimensional model and the semantic layer.

```
dbt_project.yml, profiles.yml     project and connection (credentials from env vars)
models/bronze/sources.yml         Bronze tables as dbt sources (built by ingestion/to_bronze)
models/silver/<source_system>/    typed, cleaned, conformed source tables
models/gold/                      conformed dimensions and facts
semantic_models/                  MetricFlow semantic models and metrics
dimensions/                       dimension registry (definitions-as-code, draft; docs/model.md)
macros/  seeds/  snapshots/  analyses/
warehouse.yaml                    activities the Pipeline may run
```

Proven on 12 October 2026: dbt-duckdb reads Bronze and writes Iceberg tables to the R2
catalog (readable by DuckDB and R2 SQL). Iceberg tables cannot be renamed in the
transaction that created them, so the profile commits each statement separately; a
table rebuild is therefore not atomic. When the catalog's rate limit (HTTP 429) fails a
build, `build.py` resumes it with `dbt retry` on fewer threads (`warehouse.yaml`
`rate_limit_retry_*`), which also replaces tables a failed rename left as `__dbt_backup`.

The scheduled build (empty filter) rebuilds only models whose inputs or code changed
(owner policy #822, `changed.py`): a model is selected when its table is missing, its
code fingerprint (SQL, config, macros, dimensions, `dbt_project.yml`, requirements)
differs, an upstream model is rebuilt, or a Bronze input or upstream table changed after
its last build started. The scheduled build records `zohelo.built_from_ms` and
`zohelo.build_fingerprint` as table properties of each model it wrote whose tests passed
(`pass` or `warn`: a warn-severity test such as `bdl_observations.year` not_null counts as passed);
models it did not finish or whose tests failed get none, so they are rebuilt (and tested)
next time; tests of unchanged models run when the model next rebuilds. The
recorded start is moved back 10 minutes to cover Bronze commits that become visible late.
A manual run (any filter; `fqn:*` builds everything) writes no properties, so the next
scheduled run rebuilds what it built, once. A model table changed by anything but dbt
(other than compaction) is not noticed downstream: only dbt writes Silver and Gold.

Incremental models (`materialized='incremental'`; today `silver.bdl_observations`, ~229 M
rows) are rebuilt incrementally when selected. `bdl_observations` keys each row by its
recipe: subgroup, Bronze file (`raw_sha256`) and `variables_key` (digest of the subgroup's
variable mapping in `bdl_variables`). Its pre-hook deletes rows whose recipe is no longer
current, then the build inserts the current recipes it lacks; the singular test
`tests/bdl_observations_current.sql` fails if Silver holds anything else
(`macros/bdl_observations.sql`). When such a model's code fingerprint changed (its own SQL or
config, or shared code: macros, dimensions, `dbt_project.yml`, requirements) or it has no
build properties, the scheduled build rebuilds it in full once (dbt var
`full_refresh_models`, listed as `changed_only.full_refresh` in the build annotation).
Bronze rows rewritten under the same `raw_sha256` (a to_bronze re-decode) are not noticed:
such a fix must also change the model's code (e.g. a line in its header comment), so the
next scheduled build rebuilds it in full; announce it in the Work log.

Every other model is a full-rebuild table (the project default) and must be small (owner
policy #822: below ~1M rows). `warehouse.yaml` `full_rebuild` gives each one a row ceiling;
`repo_check` fails when a non-incremental model has none, when a listed model is gone or
incremental, or when a ceiling above 1M names no `exception:` issue. After a prod build,
`build.py` reads the rows of each full-rebuild model it wrote (snapshot `total-records`)
and reports them as `row_ceilings` in the build annotation: above the ceiling the
annotation is a warning; above twice the ceiling the build fails.

Target `trial` writes every model to the scratch namespace `zz_dbt_trial`. Sources not
yet modelled (GUS DBW, dane.gov.pl, GLEIF, MF Biała Lista) are still published by
`passthrough/` as copies / zero-copy references of Bronze (`silver_gold.promote`).

Eurostat Silver is too large for one build and is loaded only by announced one-off
backfills (owner policy #822): a run whose filter names the Eurostat models
(`eurostat_dataset_versions eurostat_observations`). `eurostat_batch.py` then plans the
batch from the Bronze file list (Iceberg metadata), at most 150M Bronze rows and 500
dataset versions per build; superseded versions are deleted before the insert. Files
above 700M rows are deferred. Such a run repeats batches for up to 140 minutes. Other
builds (the daily build, a `silver` filter) leave Eurostat Silver and its file index
unchanged; `eurostat_dataset_versions` reflects Bronze as of the last backfill.

```sh
cd warehouse && DBT_PROFILES_DIR=. dbt build --target trial   # needs the R2 env vars
```
