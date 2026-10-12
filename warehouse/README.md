# Warehouse

The dbt project for Bronze → Silver → Gold, the dimensional model and the semantic
layer (dbt with DuckDB and MetricFlow; created by the dbt trial). Target layout:

```
dbt_project.yml, profiles.yml     project and connection (credentials from env vars)
models/bronze/sources.yml         Bronze tables declared as dbt sources (built by ingestion/to_bronze)
models/silver/<source_system>/    typed, cleaned, conformed source tables
models/gold/                      conformed dimensions and facts
semantic_models/                  semantic models and metrics
macros/  seeds/  snapshots/
warehouse.yaml                    activities the Pipeline may run
```

Until the dbt models exist, `passthrough/` publishes Silver and Gold as copies /
zero-copy references of Bronze (`silver_gold.promote`); it is deleted when dbt
models replace those tables.
