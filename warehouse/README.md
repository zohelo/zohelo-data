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
table rebuild is therefore not atomic.

Target `trial` writes every model to the scratch namespace `zz_dbt_trial`. Until dbt
models replace them, `passthrough/` publishes Silver and Gold as copies / zero-copy
references of Bronze (`silver_gold.promote`).

```sh
cd warehouse && DBT_PROFILES_DIR=. dbt build --target trial   # needs the R2 env vars
```
