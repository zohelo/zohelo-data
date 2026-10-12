# Warehouse

Bronze → Silver → Gold, the dimensional model and the semantic layer, as one SQL
project (planned: dbt with DuckDB and MetricFlow, after a trial writing Iceberg
tables to the R2 catalog). Planned layout:

```
models/silver/<source_system>/   typed, cleaned, conformed source tables
models/gold/                     conformed dimensions and facts
semantic/                        semantic models and metrics
```

Until then `passthrough/` publishes Silver and Gold as copies / zero-copy references
of Bronze (`silver_gold.promote`). It does no cleansing or modelling.
