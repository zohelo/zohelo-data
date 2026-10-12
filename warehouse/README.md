# Warehouse

The dbt project (dbt-duckdb, R2 Data Catalog attached as `r2`): Bronze → Silver → Gold,
the dimensional model and the semantic layer.

```
dbt_project.yml, profiles.yml     project and connection (credentials from env vars)
models/bronze/sources.yml         Bronze tables as dbt sources (built by ingestion/to_bronze)
models/silver/<source_system>/    typed, cleaned, conformed source tables
models/gold/                      conformed dimensions and facts
semantic_models/                  MetricFlow semantic models and metrics
macros/  seeds/  snapshots/  analyses/
warehouse.yaml                    activities the Pipeline may run
```

`models/silver/gus_teryt/teryt_terc.sql` is the trial model (`ops/dbt_trial`, target
`trial` → scratch namespace). Until dbt models replace them, `passthrough/` publishes
Silver and Gold as copies / zero-copy references of Bronze (`silver_gold.promote`).

```sh
cd warehouse && DBT_PROFILES_DIR=. dbt build --target trial   # needs the R2 env vars
```
