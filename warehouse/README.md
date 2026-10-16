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

Target `trial` writes every model to the scratch namespace `zz_dbt_trial`. Sources not
yet modelled (GUS DBW, dane.gov.pl, GLEIF, MF Biała Lista) are still published by
`passthrough/` as copies / zero-copy references of Bronze (`silver_gold.promote`).

Eurostat Silver is too large for one build and is loaded only by announced one-off
backfills (owner policy #822): a run whose filter names the Eurostat models
(`eurostat_dataset_versions eurostat_observations`). `eurostat_batch.py` then plans the
batch from the Bronze file list (Iceberg metadata), at most 150M Bronze rows and 500
dataset versions per build; superseded versions are deleted before the insert. Files
above 700M rows are deferred. Such a run repeats batches for up to 140 minutes. Other
builds (the daily full build, a `silver` filter) leave Eurostat Silver and its file index
unchanged; `eurostat_dataset_versions` reflects Bronze as of the last backfill.

```sh
cd warehouse && DBT_PROFILES_DIR=. dbt build --target trial   # needs the R2 env vars
```
